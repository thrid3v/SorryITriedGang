import { apiFetch } from '../data/api';

export interface TextToSqlResponse {
    question: string;
    sql: string | null;
    data: Record<string, any>[];
    summary: string | null;
    row_count: number;
    error: string | null;
}

export async function askAnalyst(question: string): Promise<TextToSqlResponse> {
    const response = await apiFetch('/api/ask', {
        method: 'POST',
        body: JSON.stringify({ question })
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to get answer from analyst: ${errorText}`);
    }

    return response.json();
}
