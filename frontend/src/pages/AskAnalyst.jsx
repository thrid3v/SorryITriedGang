import { useState } from 'react';
import { askAnalyst } from '../api';

export default function AskAnalyst() {
    const [question, setQuestion] = useState('');
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState(null);
    const [history, setHistory] = useState([]);

    async function handleSubmit(e) {
        e.preventDefault();
        if (!question.trim()) return;

        setLoading(true);
        try {
            const response = await askAnalyst(question);
            setResult(response);
            setHistory([...history, response]);
            setQuestion('');
        } catch (err) {
            setResult({
                question,
                error: err.message,
                sql: null,
                data: [],
                summary: null,
                row_count: 0
            });
        } finally {
            setLoading(false);
        }
    }

    return (
        <div>
            <div className="page-header">
                <h1 className="page-title">🤖 AI Data Analyst</h1>
                <p className="page-description">
                    Ask questions in plain English — get SQL-powered answers
                </p>
            </div>

            {/* Query Input */}
            <div className="card" style={{ marginBottom: '24px' }}>
                <form onSubmit={handleSubmit}>
                    <div style={{ marginBottom: '16px' }}>
                        <label htmlFor="question" style={{
                            display: 'block',
                            marginBottom: '8px',
                            fontWeight: '600',
                            color: 'var(--text-primary)'
                        }}>
                            Your Question
                        </label>
                        <input
                            id="question"
                            type="text"
                            value={question}
                            onChange={(e) => setQuestion(e.target.value)}
                            placeholder="e.g., What are my top 5 products by revenue?"
                            disabled={loading}
                            style={{
                                width: '100%',
                                padding: '12px 16px',
                                fontSize: '16px',
                                border: '2px solid var(--border-color)',
                                borderRadius: '8px',
                                background: 'var(--bg-primary)',
                                color: 'var(--text-primary)',
                                transition: 'border-color 0.2s'
                            }}
                            onFocus={(e) => e.target.style.borderColor = '#3b82f6'}
                            onBlur={(e) => e.target.style.borderColor = 'var(--border-color)'}
                        />
                    </div>
                    <button
                        type="submit"
                        disabled={loading || !question.trim()}
                        style={{
                            background: loading ? '#9ca3af' : 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                            color: 'white',
                            border: 'none',
                            padding: '12px 32px',
                            borderRadius: '8px',
                            fontSize: '16px',
                            fontWeight: '600',
                            cursor: loading ? 'not-allowed' : 'pointer',
                            transition: 'transform 0.2s',
                            boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
                        }}
                        onMouseEnter={(e) => !loading && (e.target.style.transform = 'translateY(-2px)')}
                        onMouseLeave={(e) => e.target.style.transform = 'translateY(0)'}
                    >
                        {loading ? '🔄 Analyzing...' : '🚀 Ask'}
                    </button>
                </form>
            </div>

            {/* Example Questions */}
            {!result && (
                <div className="card" style={{ marginBottom: '24px', background: 'var(--bg-secondary)' }}>
                    <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600' }}>💡 Try asking:</h3>
                    <ul style={{ margin: 0, paddingLeft: '20px', color: 'var(--text-secondary)' }}>
                        <li style={{ marginBottom: '8px', cursor: 'pointer' }} onClick={() => setQuestion('What is my total revenue?')}>
                            What is my total revenue?
                        </li>
                        <li style={{ marginBottom: '8px', cursor: 'pointer' }} onClick={() => setQuestion('Who are my top 5 customers by spending?')}>
                            Who are my top 5 customers by spending?
                        </li>
                        <li style={{ marginBottom: '8px', cursor: 'pointer' }} onClick={() => setQuestion('Which products sell best in New York?')}>
                            Which products sell best in New York?
                        </li>
                        <li style={{ marginBottom: '8px', cursor: 'pointer' }} onClick={() => setQuestion('Show me revenue by category')}>
                            Show me revenue by category
                        </li>
                    </ul>
                </div>
            )}

            {/* Results */}
            {result && (
                <div>
                    {result.error ? (
                        <div className="card" style={{ borderLeft: '4px solid #ef4444' }}>
                            <h3 style={{ marginTop: 0, color: '#ef4444' }}>❌ Error</h3>
                            <p style={{ color: 'var(--text-secondary)' }}>{result.error}</p>
                        </div>
                    ) : (
                        <>
                            {/* Summary */}
                            <div className="card" style={{ marginBottom: '24px', background: 'linear-gradient(135deg, #667eea15 0%, #764ba215 100%)' }}>
                                <h3 style={{ marginTop: 0, fontSize: '18px', fontWeight: '600' }}>💡 Answer</h3>
                                <p style={{ fontSize: '16px', lineHeight: '1.6', margin: 0 }}>
                                    {result.summary}
                                </p>
                            </div>

                            {/* SQL Query */}
                            <div className="card" style={{ marginBottom: '24px' }}>
                                <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600' }}>📊 Generated SQL</h3>
                                <pre style={{
                                    background: 'var(--bg-secondary)',
                                    padding: '16px',
                                    borderRadius: '8px',
                                    overflow: 'auto',
                                    fontSize: '14px',
                                    lineHeight: '1.5',
                                    margin: 0
                                }}>
                                    <code>{result.sql}</code>
                                </pre>
                            </div>

                            {/* Data Table */}
                            {result.data && result.data.length > 0 && (
                                <div className="card">
                                    <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600' }}>
                                        📈 Results ({result.row_count} {result.row_count === 1 ? 'row' : 'rows'})
                                    </h3>
                                    <div style={{ overflowX: 'auto' }}>
                                        <table style={{
                                            width: '100%',
                                            borderCollapse: 'collapse',
                                            fontSize: '14px'
                                        }}>
                                            <thead>
                                                <tr style={{ background: 'var(--bg-secondary)' }}>
                                                    {Object.keys(result.data[0]).map((key) => (
                                                        <th key={key} style={{
                                                            padding: '12px',
                                                            textAlign: 'left',
                                                            fontWeight: '600',
                                                            borderBottom: '2px solid var(--border-color)'
                                                        }}>
                                                            {key}
                                                        </th>
                                                    ))}
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {result.data.slice(0, 10).map((row, idx) => (
                                                    <tr key={idx} style={{
                                                        borderBottom: '1px solid var(--border-color)'
                                                    }}>
                                                        {Object.values(row).map((val, i) => (
                                                            <td key={i} style={{ padding: '12px' }}>
                                                                {val !== null && val !== undefined ? String(val) : '-'}
                                                            </td>
                                                        ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                        {result.row_count > 10 && (
                                            <p style={{
                                                marginTop: '12px',
                                                color: 'var(--text-secondary)',
                                                fontSize: '14px',
                                                fontStyle: 'italic'
                                            }}>
                                                Showing first 10 of {result.row_count} rows
                                            </p>
                                        )}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            )}

            {/* Query History */}
            {history.length > 1 && (
                <div className="card" style={{ marginTop: '24px' }}>
                    <h3 style={{ marginTop: 0, fontSize: '16px', fontWeight: '600' }}>📜 Recent Questions</h3>
                    <ul style={{ margin: 0, paddingLeft: '20px' }}>
                        {history.slice(-5).reverse().map((item, idx) => (
                            <li key={idx} style={{
                                marginBottom: '8px',
                                color: 'var(--text-secondary)',
                                cursor: 'pointer'
                            }} onClick={() => setQuestion(item.question)}>
                                {item.question}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
        </div>
    );
}
