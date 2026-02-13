import { useState } from 'react';
import { askAnalyst, TextToSqlResponse } from '@/services/textToSql';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';

export default function AskAnalyst() {
    const [question, setQuestion] = useState('');
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<TextToSqlResponse | null>(null);
    const [history, setHistory] = useState<TextToSqlResponse[]>([]);

    async function handleSubmit(e: React.FormEvent) {
        e.preventDefault();
        if (!question.trim()) return;

        setLoading(true);
        try {
            const response = await askAnalyst(question);
            setResult(response);
            setHistory(prev => [...prev, response]);
            setQuestion('');
        } catch (err: any) {
            setResult({
                question,
                error: err.message || "An error occurred",
                sql: null,
                data: [],
                summary: null,
                row_count: 0
            });
        } finally {
            setLoading(false);
        }
    }

    const handleHistoryClick = (q: string) => {
        setQuestion(q);
    };

    return (
        <div className="space-y-8 max-w-5xl mx-auto">
            <div className="flex flex-col space-y-2">
                <h1 className="text-3xl font-bold tracking-tight">🤖 AI Data Analyst</h1>
                <p className="text-muted-foreground">
                    Ask questions in plain English — get SQL-powered answers
                </p>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle>Ask a Question</CardTitle>
                    <CardDescription>Query your data in plain English</CardDescription>
                </CardHeader>
                <CardContent>
                    <form onSubmit={handleSubmit} className="flex gap-4">
                        <Input
                            value={question}
                            onChange={(e) => setQuestion(e.target.value)}
                            placeholder="e.g., What is my total revenue?"
                            disabled={loading}
                            className="flex-1"
                        />
                        <Button type="submit" disabled={loading || !question.trim()}>
                            {loading ? 'Analyzing...' : 'Ask'}
                        </Button>
                    </form>
                </CardContent>
            </Card>

            {!result && (
                <Card className="bg-muted/50">
                    <CardHeader>
                        <CardTitle className="text-sm font-medium">💡 Try asking:</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <ul className="list-disc pl-5 space-y-2 text-sm text-muted-foreground">
                            <li className="cursor-pointer hover:text-primary transition-colors" onClick={() => setQuestion('What is my total revenue?')}>
                                What is my total revenue?
                            </li>
                            <li className="cursor-pointer hover:text-primary transition-colors" onClick={() => setQuestion('Who are my top 5 customers by spending?')}>
                                Who are my top 5 customers by spending?
                            </li>
                            <li className="cursor-pointer hover:text-primary transition-colors" onClick={() => setQuestion('Which products sell best in New York?')}>
                                Which products sell best in New York?
                            </li>
                            <li className="cursor-pointer hover:text-primary transition-colors" onClick={() => setQuestion('Show me revenue by category')}>
                                Show me revenue by category
                            </li>
                        </ul>
                    </CardContent>
                </Card>
            )}

            {result && (
                <div className="space-y-6">
                    {result.error ? (
                        <Card className="border-destructive">
                            <CardHeader>
                                <CardTitle className="text-destructive">❌ Error</CardTitle>
                            </CardHeader>
                            <CardContent>
                                <p className="text-muted-foreground">{result.error}</p>
                            </CardContent>
                        </Card>
                    ) : (
                        <>
                            {result.summary && (
                                <Card className="bg-primary/5 border-primary/20">
                                    <CardHeader>
                                        <CardTitle className="text-lg">💡 Answer</CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <p className="leading-relaxed">{result.summary}</p>
                                    </CardContent>
                                </Card>
                            )}

                            <Card>
                                <CardHeader>
                                    <CardTitle className="text-base">📊 Generated SQL</CardTitle>
                                </CardHeader>
                                <CardContent>
                                    <pre className="bg-muted p-4 rounded-md overflow-x-auto text-sm font-mono">
                                        <code>{result.sql}</code>
                                    </pre>
                                </CardContent>
                            </Card>

                            {result.data && result.data.length > 0 && (
                                <Card>
                                    <CardHeader>
                                        <CardTitle className="text-base">
                                            📈 Results ({result.row_count} {result.row_count === 1 ? 'row' : 'rows'})
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent>
                                        <div className="rounded-md border">
                                            <Table>
                                                <TableHeader>
                                                    <TableRow>
                                                        {Object.keys(result.data[0]).map((key) => (
                                                            <TableHead key={key} className="font-bold">{key}</TableHead>
                                                        ))}
                                                    </TableRow>
                                                </TableHeader>
                                                <TableBody>
                                                    {result.data.slice(0, 10).map((row, idx) => (
                                                        <TableRow key={idx}>
                                                            {Object.values(row).map((val, i) => (
                                                                <TableCell key={i}>
                                                                    {val !== null && val !== undefined ? String(val) : '-'}
                                                                </TableCell>
                                                            ))}
                                                        </TableRow>
                                                    ))}
                                                </TableBody>
                                            </Table>
                                        </div>
                                        {result.row_count > 10 && (
                                            <p className="mt-4 text-sm text-muted-foreground italic">
                                                Showing first 10 of {result.row_count} rows
                                            </p>
                                        )}
                                    </CardContent>
                                </Card>
                            )}
                        </>
                    )}
                </div>
            )}

            {history.length > 0 && (
                <div className="pt-8">
                    <h3 className="text-lg font-semibold mb-4">📜 Recent Questions</h3>
                    <ul className="space-y-2">
                        {history.slice(-5).reverse().map((item, idx) => (
                            <li key={idx}
                                onClick={() => handleHistoryClick(item.question)}
                                className="text-muted-foreground hover:text-primary cursor-pointer text-sm transition-colors"
                            >
                                {item.question}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
        </div>
    );
}
