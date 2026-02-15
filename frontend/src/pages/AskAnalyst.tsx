import { useState } from 'react';
import { askAnalyst, TextToSqlResponse } from '@/services/textToSql';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Sparkles } from 'lucide-react';
import { cn } from '@/lib/utils';
import { StatCard, Leaderboard, DataTable } from '@/components/analyst/DataComponents';
import { ActionCard, SessionHistory, SystemStatus, quickActions } from '@/components/analyst/MissionControlComponents';

export default function AskAnalyst() {
    const [question, setQuestion] = useState('');
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState<TextToSqlResponse | null>(null);
    const [history, setHistory] = useState<Array<{ question: string; timestamp: Date; response: TextToSqlResponse }>>([]);

    async function handleSubmit(e?: React.FormEvent) {
        if (e) e.preventDefault();
        if (!question.trim()) return;

        setLoading(true);
        try {
            const response = await askAnalyst(question);
            setResult(response);
            setHistory(prev => [...prev, { question, timestamp: new Date(), response }]);
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

    const handleQuickAction = (actionQuestion: string) => {
        setQuestion(actionQuestion);
        setTimeout(() => {
            const form = document.querySelector('form');
            if (form) form.requestSubmit();
        }, 100);
    };

    const handleHistoryClick = (q: string) => {
        setQuestion(q);
    };

    // Detect data type and render appropriate component
    const renderDataResponse = () => {
        if (!result || result.error || !result.data || result.data.length === 0) return null;

        // Single metric (1 row, 1-2 columns)
        if (result.data.length === 1 && Object.keys(result.data[0]).length <= 2) {
            const keys = Object.keys(result.data[0]);
            const valueKey = keys.find(k => typeof result.data[0][k] === 'number') || keys[0];
            const value = result.data[0][valueKey];

            return (
                <StatCard
                    label={result.question}
                    value={typeof value === 'number' ? `$${(value / 1000000).toFixed(2)}M` : String(value)}
                />
            );
        }

        // Leaderboard (list with numeric values)
        const keys = Object.keys(result.data[0]);
        const hasNumericValue = keys.some(k => typeof result.data[0][k] === 'number');

        if (hasNumericValue && result.data.length > 1 && result.data.length <= 10) {
            const labelKey = keys.find(k => typeof result.data[0][k] === 'string') || keys[0];
            const valueKey = keys.find(k => typeof result.data[0][k] === 'number') || keys[1];
            const maxValue = Math.max(...result.data.map(row => Number(row[valueKey]) || 0));

            const items = result.data.map(row => ({
                label: String(row[labelKey]),
                value: Number(row[valueKey]) || 0,
                maxValue
            }));

            return <Leaderboard items={items} />;
        }

        // Default: Data table
        return <DataTable data={result.data} />;
    };

    return (
        <div className="h-full flex flex-col">
            {/* System Status Strip */}
            <div className="mb-4">
                <SystemStatus sources={[
                    { name: "Inventory", status: "live" },
                    { name: "Sales", status: "live" },
                    { name: "Customers", status: "live" }
                ]} />
            </div>

            {/* 2-Column Layout - Removed 3rd column to reduce congestion */}
            <div className="flex gap-6 flex-1">
                {/* Main Column (75%) - Workspace */}
                <div className="flex-[0_0_75%] flex flex-col gap-6">
                    {/* Hero Card - Ask Question */}
                    <div className="bento-card p-6">
                        <div className="flex items-center gap-3 mb-4">
                            <div className="flex items-center justify-center w-10 h-10 rounded-full bg-acid-lime/10">
                                <Sparkles className="h-5 w-5 text-acid-lime" />
                            </div>
                            <div>
                                <h1 className="text-xl font-bold gradient-text">AI Data Analyst</h1>
                                <p className="text-xs text-muted-foreground">Ask questions in plain English</p>
                            </div>
                        </div>

                        <form onSubmit={handleSubmit} className="flex gap-3">
                            <Input
                                value={question}
                                onChange={(e) => setQuestion(e.target.value)}
                                placeholder="e.g., What is my total revenue?"
                                disabled={loading}
                                className="flex-1 rounded-pill h-11 px-5 bg-deep-charcoal border-border/30 focus:border-acid-lime"
                            />
                            <Button
                                type="submit"
                                disabled={loading || !question.trim()}
                                className="pill-button h-11 px-6"
                            >
                                {loading ? 'Analyzing...' : 'Ask'}
                            </Button>
                        </form>
                    </div>

                    {/* Result Stage */}
                    {result && (
                        <div className="space-y-4">
                            {result.error ? (
                                <div className="bento-card p-5 border-2 border-safety-orange/30">
                                    <h3 className="condensed-header text-safety-orange mb-2">❌ Error</h3>
                                    <p className="text-muted-foreground leading-relaxed text-sm">{result.error}</p>
                                </div>
                            ) : (
                                <>
                                    {/* Answer Summary */}
                                    {result.summary && (
                                        <div className="bento-card p-5 bg-acid-lime/5 border border-acid-lime/20">
                                            <h3 className="condensed-header text-acid-lime mb-2">💡 Answer</h3>
                                            <p className="text-zinc-300 leading-relaxed">{result.summary}</p>
                                        </div>
                                    )}

                                    {/* Data Visualization */}
                                    {renderDataResponse()}

                                    {/* SQL Query */}
                                    <div className="bento-card p-5">
                                        <h3 className="condensed-header text-foreground mb-3">📊 Generated SQL</h3>
                                        <pre className="bg-deep-charcoal p-4 rounded-card overflow-x-auto text-xs font-mono border border-border/30">
                                            <code className="text-acid-lime">{result.sql}</code>
                                        </pre>
                                    </div>
                                </>
                            )}
                        </div>
                    )}
                </div>

                {/* Right Column (25%) - Session History */}
                <div className="flex-[0_0_25%]">
                    <div className="bento-card p-5 h-full max-h-[600px] overflow-y-auto custom-scrollbar">
                        <h3 className="condensed-header text-foreground mb-4">📜 History</h3>
                        <SessionHistory
                            history={history.map(h => ({ question: h.question, timestamp: h.timestamp }))}
                            onSelect={handleHistoryClick}
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}
