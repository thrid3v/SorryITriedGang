import { apiFetch } from '@/data/api';

export interface TextToSqlResponse {
    question: string;
    sql: string | null;
    data: Record<string, any>[];
    summary: string | null;
    row_count: number;
    error: string | null;
}

export async function askAnalyst(question: string): Promise<TextToSqlResponse> {
    // Use the chat endpoint, which accepts a JSON body { question }.
    const response = await apiFetch('/api/chat/ask', {
        method: 'POST',
        body: JSON.stringify({ question })
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to get answer from analyst: ${errorText}`);
    }

    return response.json();
}
