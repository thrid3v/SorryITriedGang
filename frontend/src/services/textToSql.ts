const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export interface TextToSqlResponse {
    question: string;
    sql: string | null;
    data: Record<string, any>[];
    summary: string | null;
    row_count: number;
    error: string | null;
}

export async function askAnalyst(question: string): Promise<TextToSqlResponse> {
    const response = await fetch(`${API_BASE_URL}/api/chat/ask`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question })
    });

    if (!response.ok) {
        throw new Error("Failed to get answer from analyst");
    }

    return response.json();
}
