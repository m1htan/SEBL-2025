import os
import pandas as pd
from typing import TypedDict, List, Dict

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import ChatPromptTemplate

load_dotenv(dotenv_path='/Users/minhtan/Documents/GitHub/SEBL-2025/config/config.env')
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash-lite", temperature=0.7, google_api_key=os.environ["GOOGLE_API_KEY"])

# 1. Định nghĩa State ------------------------------------------------------------------------------------
class CountryState(TypedDict):
    group_id: int
    file_path: str
    raw_data: List[Dict]
    low_score_countries: List[Dict]
    feedbacks: List[Dict]
    suggestions: List[Dict]
    report: List[Dict]

def load_or_run_pipeline(output_path: str) -> bool:
    """Kiểm tra xem có cần chạy lại pipeline hay không."""
    if os.path.exists(output_path):
        try:
            df = pd.read_csv(output_path)
            if not df.empty:
                print(f"[SKIP] Báo cáo '{output_path}' đã tồn tại và có dữ liệu.")
                return False
            else:
                print(f"[INFO] File báo cáo '{output_path}' đã tồn tại nhưng rỗng – sẽ chạy lại.")
                return True
        except Exception as e:
            print(f"[WARNING] Lỗi đọc file '{output_path}': {e} – sẽ chạy lại.")
            return True
    return True  # File chưa tồn tại

# 2. Tạo các hàm xử lý dưới dạng tool node --------------------------------------------------------------
def load_csv_tool(state: CountryState) -> CountryState:
    try:
        df = pd.read_csv(state["file_path"])
        if df.empty:
            raise ValueError("File CSV trống.")
        return {**state, "raw_data": df.to_dict(orient="records")}
    except Exception as e:
        print(f"[ERROR] Lỗi khi load file {state['file_path']}: {e}")
        return {**state, "raw_data": []}

def filter_low_tool(state: CountryState) -> CountryState:
    try:
        data = state["raw_data"]
        filtered = [
            d for d in data
            if d.get("position_vs_baseline", "").lower() in ["below", "equal", "0", "1", "2"]
        ]
        return {**state, "low_score_countries": filtered}
    except Exception as e:
        print(f"[ERROR] Lỗi khi lọc quốc gia position_vs_baseline thấp: {e}")
        return {**state, "low_score_countries": []}

def analyze_tool(state: CountryState) -> CountryState:
    try:
        feedbacks = []
        for c in state["low_score_countries"]:
            score = float(c.get("scaled_score", -1))
            name = c.get("country_code", "Unknown")
            fb = f"{name} chỉ đạt {score:.2f}/10. "
            if score < 3:
                fb += "Điểm rất thấp – cần cải tổ toàn diện."
            elif score < 5:
                fb += "Cần tập trung cải thiện các yếu tố then chốt."
            else:
                fb += "Tạm ổn nhưng vẫn có thể cải thiện."
            feedbacks.append({
                "country_code": name,
                "scaled_score": score,
                "feedback": fb
            })
        return {**state, "feedbacks": feedbacks}
    except Exception as e:
        print(f"[ERROR] Lỗi khi phân tích điểm quốc gia: {e}")
        return {**state, "feedbacks": []}

def suggest_tool(state: CountryState) -> CountryState:
    try:
        prompt_template = ChatPromptTemplate.from_messages([
            ("system", """Bạn là chuyên gia chính sách công và là chuyên gia tư vấn chính sách phát triển thị trường xanh.
Nhiệm vụ của bạn là đưa ra đề xuất chính sách cho từng quốc gia dựa trên điểm số đánh giá hiện tại và vị trí so với baseline.
Các yếu tố bao gồm:
- `scaled_score`: điểm số từ 0–10,
- `position_vs_baseline`: vị trí so với mặt bằng chung ('below', 'equal', 'above'),
- `country_code`: tên quốc gia.

Yêu cầu:
- Đề xuất phải cụ thể, thực tế, phù hợp với năng lực của quốc gia đó.
- Dài khoảng 2–3 câu.
- Sử dụng giọng văn chính sách, khích lệ và tập trung vào hành động."""),
            ("human", "Dưới đây là thông tin quốc gia cần bạn tư vấn:\n\n{country_info}\n\nVui lòng viết đề xuất.")
        ])

        suggestions = []
        for c in state["low_score_countries"]:
            country_info = f"""
Tên quốc gia: {c.get("country_code", "Unknown")}
Điểm: {c.get("scaled_score", "N/A")}
Vị trí so với baseline: {c.get("position_vs_baseline", "unknown")}
"""
            chain = prompt_template | llm
            res = chain.invoke({"country_info": country_info})
            suggestions.append({
                "country_code": c.get("country_code", "Unknown"),
                "suggestion": res.content.strip()
            })

        return {**state, "suggestions": suggestions}

    except Exception as e:
        print(f"[ERROR] Lỗi khi sinh gợi ý từ Gemini: {e}")
        return {**state, "suggestions": []}


def report_tool(state: CountryState) -> CountryState:
    try:
        report = []
        for f, s in zip(state["feedbacks"], state["suggestions"]):
            report.append({
                "country_code": f["country_code"],
                "scaled_score": f["scaled_score"],
                "feedback": f["feedback"],
                "suggestion": s["suggestion"]
            })
        return {**state, "report": report}
    except Exception as e:
        print(f"[ERROR] Lỗi khi tổng hợp báo cáo: {e}")
        return {**state, "report": []}

def save_tool(state: CountryState) -> CountryState:
    try:
        group_id = state["group_id"]
        df = pd.DataFrame(state["report"])
        os.makedirs("reports", exist_ok=True)
        path = f"reports/group{group_id}_report.csv"
        df.to_csv(path, index=False)
        print(f"[OK] Saved report to {path}")
        return state
    except Exception as e:
        print(f"[ERROR] Lỗi khi lưu báo cáo CSV: {e}")
        return state

# 3. Tạo graph LangGraph ---------------------------------------------------------------------------------
graph = StateGraph(CountryState)

graph.add_node("load_csv", load_csv_tool)
graph.add_node("filter_low", filter_low_tool)
graph.add_node("analyze", analyze_tool)
graph.add_node("suggest", suggest_tool)
graph.add_node("report", report_tool)
graph.add_node("save", save_tool)

graph.set_entry_point("load_csv")
graph.add_edge("load_csv", "filter_low")
graph.add_edge("filter_low", "analyze")
graph.add_edge("analyze", "suggest")
graph.add_edge("suggest", "report")
graph.add_edge("report", "save")
graph.set_finish_point("save")

compiled = graph.compile()

# 4. Gọi graph cho từng nhóm -------------------------------------------------------------------------------
if __name__ == "__main__":
    for group_id in range(1, 5):
        input_path = f"/Users/minhtan/Documents/GitHub/SEBL-2025/output/compare/ai_agent_country_zone_group{group_id}.csv"
        output_path = f"reports/group{group_id}_report.csv"

        if not os.path.exists(input_path):
            print(f"[SKIP] File {input_path} không tồn tại.")
            continue

        if not load_or_run_pipeline(output_path):
            continue

        if os.path.exists(output_path):
            try:
                df_check = pd.read_csv(output_path)
                if not df_check.empty:
                    print(f"[SKIP] Báo cáo cho group {group_id} đã tồn tại và có dữ liệu, bỏ qua xử lý.")
                    continue
                else:
                    print(f"[INFO] Báo cáo cho group {group_id} đã có file nhưng rỗng, sẽ chạy lại.")
            except Exception as e:
                print(f"[WARNING] Không đọc được file {output_path} do lỗi: {e}, sẽ chạy lại.")

        print(f"[INFO] Đang chạy nhóm {group_id}...")
        try:
            compiled.invoke({
                "group_id": group_id,
                "file_path": input_path
            })
        except Exception as e:
            print(f"[ERROR] Lỗi khi chạy LangGraph cho group {group_id}: {e}")
