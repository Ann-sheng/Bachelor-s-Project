import * as React from "react";
const { useState, useRef, useEffect } = React;

// ── Error Boundary ────────────────────────────────────────────────────────────

class ErrorBoundary extends React.Component<
    { children: React.ReactNode },
    { hasError: boolean; error: string }
> {
    constructor(props: any) {
        super(props);
        this.state = { hasError: false, error: "" };
    }
    static getDerivedStateFromError(error: any) {
        return { hasError: true, error: String(error) };
    }
    render() {
        if (this.state.hasError) {
            return (
                <div style={{
                    padding: "20px", background: "#2C1810",
                    color: "#F5E6C8", fontFamily: "Segoe UI, sans-serif"
                }}>
                    <div style={{ color: "#C84B11", fontWeight: 700, marginBottom: "8px" }}>
                        Something went wrong
                    </div>
                    <div style={{ fontSize: "12px", color: "#8B6050" }}>
                        {this.state.error}
                    </div>
                    <button
                        onClick={() => this.setState({ hasError: false, error: "" })}
                        style={{
                            marginTop: "12px", background: "#C84B11",
                            color: "#F5E6C8", border: "none", borderRadius: "6px",
                            padding: "6px 14px", cursor: "pointer"
                        }}>
                        Try again
                    </button>
                </div>
            );
        }
        return this.props.children;
    }
}

// ── Types ─────────────────────────────────────────────────────────────────────

interface Message {
    role: "user" | "assistant";
    content: string;
    sql?: string;
    rowCount?: number;
    truncated?: boolean;
    error?: string;
}

interface HistoryEntry {
    role: "user" | "assistant";
    content: string;
}

interface ResultRow {
    [key: string]: any;
}

// ── Persistent state — survives Power BI tab switching ────────────────────────
// Module-level vars reset when PBI destroys the iframe. localStorage survives.

const STORAGE_KEY = "sqlchat_v1";

const loadState = () => {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        return raw ? JSON.parse(raw) : null;
    } catch { return null; }
};

const saveState = (patch: object) => {
    try {
        const current = loadState() ?? {};
        localStorage.setItem(STORAGE_KEY, JSON.stringify({ ...current, ...patch }));
    } catch { /* quota exceeded — silent fail */ }
};

const saved = loadState();

// ── Main component ────────────────────────────────────────────────────────────

const ChatVisualInner: React.FC = () => {
    const [messages,  setMessages]  = useState<Message[]>(saved?.messages ?? []);
    const [history,   setHistory]   = useState<HistoryEntry[]>(saved?.history ?? []);
    const [rows,      setRows]      = useState<ResultRow[]>(saved?.rows ?? []);
    const [columns,   setColumns]   = useState<string[]>(saved?.columns ?? []);
    const [lastRun,   setLastRun]   = useState<string | null>(saved?.lastRun ?? null);
    const [input,     setInput]     = useState("");
    const [loading,   setLoading]   = useState(false);
    const [csvCopied, setCsvCopied] = useState(false);
    const [csvModal,  setCsvModal]  = useState<string | null>(null);
    const chatEndRef                = useRef<HTMLDivElement>(null);
    const abortControllerRef        = useRef<AbortController | null>(null);

    // Mirror state to localStorage so it survives iframe teardown
    useEffect(() => { saveState({ messages }); }, [messages]);
    useEffect(() => { saveState({ history });  }, [history]);
    useEffect(() => { saveState({ rows });     }, [rows]);
    useEffect(() => { saveState({ columns });  }, [columns]);
    useEffect(() => { saveState({ lastRun });  }, [lastRun]);

    useEffect(() => {
        chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }, [messages]);

    // ── Send ─────────────────────────────────────────────────────────────────

    const handleSend = async () => {
        const question = input.trim();
        if (!question || loading) return;

        const controller = new AbortController();
        abortControllerRef.current = controller;

        setMessages(prev => [...prev, { role: "user", content: question }]);
        setInput("");
        setLoading(true);

        try {
            const response = await fetch("http://localhost:800/query", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ question, history }),
                signal: controller.signal,
            });

            let data: any = {};
            try { data = await response.json(); } catch { data = {}; }

            if (!response.ok) {
                const errMsg = typeof data?.detail === "string"
                    ? data.detail
                    : `Server error ${response.status}`;
                setMessages(prev => [...prev, {
                    role: "assistant" as const, content: "", error: errMsg
                }]);
                setLoading(false);
                abortControllerRef.current = null;
                return;
            }

            const sql: string | null =
                data.sql && typeof data.sql === "string" && data.sql.trim() !== ""
                    ? data.sql.trim()
                    : null;

            const resultRows: ResultRow[] = Array.isArray(data.rows) ? data.rows : [];
            const truncated: boolean = !!data.truncated;

            setMessages(prev => [...prev, {
                role: "assistant" as const,
                content: "",
                sql: sql ?? undefined,
                rowCount: resultRows.length,
                truncated,
                error: sql ? undefined : "Model returned empty SQL — try rephrasing.",
            }]);

            if (sql) {
                setHistory(prev => [
                    ...prev,
                    { role: "user",      content: question },
                    { role: "assistant", content: sql },
                ]);
            }

            if (resultRows.length > 0) {
                setRows(resultRows);
                setColumns(Object.keys(resultRows[0]));
                setLastRun("just now");
            }

        } catch (err: any) {
            const isAbort = err?.name === "AbortError";
            setMessages(prev => [...prev, {
                role: "assistant" as const,
                content: "",
                error: isAbort
                    ? "Request cancelled."
                    : "Could not reach FastAPI. Is it running on localhost:800?",
            }]);
        }

        abortControllerRef.current = null;
        setLoading(false);
    };

    // ── Abort ────────────────────────────────────────────────────────────────

    const handleAbort = () => {
        abortControllerRef.current?.abort();
    };

    // ── Clear ────────────────────────────────────────────────────────────────

    const handleClear = () => {
        abortControllerRef.current?.abort();
        localStorage.removeItem(STORAGE_KEY);
        setMessages([]);
        setHistory([]);
        setRows([]);
        setColumns([]);
        setLastRun(null);
        setInput("");
        setCsvModal(null);
    };

    // ── Keyboard ─────────────────────────────────────────────────────────────

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    // ── CSV export ────────────────────────────────────────────────────────────

    const buildCsv = (): string => {
        const header = columns.join(",");
        const body = rows.map(row =>
            columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) return "";
                const str = String(val);
                return str.includes(",") || str.includes('"') || str.includes("\n")
                    ? `"${str.replace(/"/g, '""')}"`
                    : str;
            }).join(",")
        ).join("\n");
        return header + "\n" + body;
    };

    const handleExportCSV = async () => {
        if (rows.length === 0) return;
        const csv = buildCsv();

        // Primary: clipboard — works in most PBI contexts on a user gesture
        try {
            await navigator.clipboard.writeText(csv);
            setCsvCopied(true);
            setTimeout(() => setCsvCopied(false), 2500);
            return;
        } catch {
            // Clipboard blocked — fall through to modal
        }

        // Fallback: show CSV in a copyable textarea modal
        setCsvModal(csv);
    };

    // ── Styles ────────────────────────────────────────────────────────────────

    const s = {
        container: {
            position: "relative" as const,
            display: "flex", flexDirection: "column" as const,
            height: "100%", fontFamily: "Segoe UI, sans-serif",
            fontSize: "13px", background: "#2C1810", color: "#F5E6C8",
            boxSizing: "border-box" as const,
        },
        header: {
            display: "flex", justifyContent: "space-between",
            alignItems: "center", padding: "8px 14px",
            background: "#231208", borderBottom: "1px solid #5C3020",
            flexShrink: 0,
        },
        headerTitle: {
            fontWeight: 700, fontSize: "14px",
            color: "#F5E6C8", letterSpacing: "0.5px",
        },
        clearBtn: {
            background: "none", border: "1px solid #5C3020",
            color: "#F5E6C8", borderRadius: "4px",
            padding: "3px 10px", cursor: "pointer", fontSize: "12px",
        },
        body: { display: "flex", flex: 1, overflow: "hidden" },
        leftPane: {
            flex: 1, display: "flex", flexDirection: "column" as const,
            borderRight: "1px solid #5C3020", overflow: "hidden",
        },
        messages: {
            flex: 1, overflowY: "auto" as const, padding: "12px",
            display: "flex", flexDirection: "column" as const, gap: "10px",
        },
        userBubble: {
            alignSelf: "flex-end" as const, background: "#C84B11",
            color: "#F5E6C8", borderRadius: "12px 12px 2px 12px",
            padding: "7px 12px", maxWidth: "80%",
        },
        assistantBlock: {
            alignSelf: "flex-start" as const, maxWidth: "95%",
            display: "flex", flexDirection: "column" as const, gap: "5px",
        },
        sqlBlock: {
            background: "#231208", border: "1px solid #5C3020",
            borderRadius: "6px", padding: "8px 10px",
            fontFamily: "Consolas, monospace", fontSize: "11px",
            color: "#E8C99A", whiteSpace: "pre-wrap" as const,
            wordBreak: "break-all" as const,
        },
        badges: { display: "flex", gap: "6px", flexWrap: "wrap" as const },
        badgeOk: {
            background: "#C84B11", color: "#F5E6C8",
            borderRadius: "4px", padding: "2px 7px",
            fontSize: "11px", fontWeight: 600,
        },
        badgeRows: {
            background: "#8B4513", color: "#F5E6C8",
            borderRadius: "4px", padding: "2px 7px",
            fontSize: "11px", fontWeight: 600,
        },
        badgeTruncated: {
            background: "#7B3800", color: "#FFD580",
            borderRadius: "4px", padding: "2px 7px",
            fontSize: "11px", fontWeight: 600,
        },
        badgeError: {
            background: "#7B1C1C", color: "#F5E6C8",
            borderRadius: "4px", padding: "4px 8px",
            fontSize: "11px", fontWeight: 600,
            maxWidth: "95%", wordBreak: "break-word" as const, lineHeight: "1.5",
        },
        thinkingText: { color: "#8B6050", fontSize: "12px" },
        abortBtn: {
            background: "none", border: "1px solid #5C3020",
            color: "#F5E6C8", borderRadius: "20px",
            padding: "5px 16px", cursor: "pointer", fontSize: "12px",
            display: "inline-flex", alignItems: "center", gap: "6px",
            marginTop: "4px", alignSelf: "flex-start" as const,
        },
        abortSquare: {
            width: "8px", height: "8px",
            background: "#F5E6C8", borderRadius: "1px",
            display: "inline-block", flexShrink: 0,
        },
        inputRow: {
            display: "flex", gap: "6px", padding: "10px",
            borderTop: "1px solid #5C3020", flexShrink: 0,
        },
        input: {
            flex: 1, background: "#231208", border: "1px solid #5C3020",
            borderRadius: "6px", color: "#F5E6C8", padding: "7px 10px",
            fontSize: "13px", outline: "none",
        },
        sendBtn: {
            background: "#C84B11", color: "#F5E6C8", border: "none",
            borderRadius: "6px", padding: "7px 14px",
            cursor: "pointer", fontWeight: 700, fontSize: "13px",
        },
        rightPane: {
            width: "45%", display: "flex", flexDirection: "column" as const,
            overflow: "hidden", flexShrink: 0,
        },
        tableWrap: { flex: 1, overflowY: "auto" as const, padding: "10px" },
        table: { width: "100%", borderCollapse: "collapse" as const, fontSize: "12px" },
        th: {
            background: "#3D1F10", color: "#C84B11",
            padding: "6px 8px", textAlign: "left" as const,
            borderBottom: "1px solid #5C3020", fontWeight: 700,
        },
        td: {
            padding: "5px 8px", borderBottom: "1px solid #3D1F10", color: "#F5E6C8",
        },
        rightFooter: {
            padding: "8px 12px", borderTop: "1px solid #5C3020", flexShrink: 0,
            display: "flex", justifyContent: "space-between", alignItems: "center",
        },
        meta:      { fontSize: "11px", color: "#8B6050" },
        exportBtn: {
            background: "#C84B11", color: "#F5E6C8", border: "none",
            borderRadius: "6px", padding: "5px 12px",
            cursor: "pointer", fontWeight: 700, fontSize: "12px",
        },
        empty: {
            color: "#5C3020", fontSize: "12px",
            padding: "20px", textAlign: "center" as const,
        },
        modalOverlay: {
            position: "absolute" as const, inset: 0,
            background: "rgba(0,0,0,0.72)",
            display: "flex", alignItems: "center", justifyContent: "center",
            zIndex: 999,
        },
        modalCard: {
            background: "#2C1810", border: "1px solid #5C3020",
            borderRadius: "8px", padding: "16px",
            width: "90%", maxWidth: "480px",
            display: "flex", flexDirection: "column" as const, gap: "10px",
        },
        modalHeader: {
            display: "flex", justifyContent: "space-between", alignItems: "center",
        },
        modalTitle: {
            fontWeight: 700, color: "#F5E6C8", fontSize: "13px",
        },
        modalClose: {
            background: "none", border: "none",
            color: "#8B6050", cursor: "pointer", fontSize: "18px", lineHeight: "1",
        },
        modalTextarea: {
            width: "100%", height: "160px",
            background: "#231208", border: "1px solid #5C3020",
            borderRadius: "6px", color: "#E8C99A",
            fontFamily: "Consolas, monospace", fontSize: "11px",
            padding: "8px", resize: "none" as const,
            boxSizing: "border-box" as const,
        },
        modalFooter: {
            display: "flex", gap: "8px", justifyContent: "flex-end",
        },
        modalSelectBtn: {
            background: "#3D1F10", color: "#F5E6C8", border: "1px solid #5C3020",
            borderRadius: "6px", padding: "5px 12px",
            cursor: "pointer", fontSize: "12px",
        },
        modalDoneBtn: {
            background: "#C84B11", color: "#F5E6C8", border: "none",
            borderRadius: "6px", padding: "5px 12px",
            cursor: "pointer", fontWeight: 700, fontSize: "12px",
        },
    };

    // ── Render ────────────────────────────────────────────────────────────────

    return (
        <div style={s.container}>
            {/* Header */}
            <div style={s.header}>
                <span style={s.headerTitle}>SQL GENERATOR</span>
                <button style={s.clearBtn} onClick={handleClear}>clear</button>
            </div>

            <div style={s.body}>
                {/* ── Left pane: chat ── */}
                <div style={s.leftPane}>
                    <div style={s.messages}>
                        {messages.length === 0 && (
                            <div style={s.empty}>Type a question to get started</div>
                        )}

                        {messages.map((msg, i) => (
                            <div key={i}>
                                {msg.role === "user" ? (
                                    <div style={s.userBubble}>{msg.content}</div>
                                ) : (
                                    <div style={s.assistantBlock}>
                                        {msg.error ? (
                                            <div style={s.badgeError}>{msg.error}</div>
                                        ) : (
                                            <>
                                                <div style={s.sqlBlock}>{msg.sql}</div>
                                                <div style={s.badges}>
                                                    <span style={s.badgeOk}>query ok</span>
                                                    <span style={s.badgeRows}>{msg.rowCount} rows</span>
                                                    {msg.truncated && (
                                                        <span style={s.badgeTruncated}>
                                                            ⚠ capped at 500 — refine your question
                                                        </span>
                                                    )}
                                                </div>
                                            </>
                                        )}
                                    </div>
                                )}
                            </div>
                        ))}

                        {loading && (
                            <div style={s.assistantBlock}>
                                <span style={s.thinkingText}>thinking...</span>
                                <button style={s.abortBtn} onClick={handleAbort}>
                                    <span style={s.abortSquare} />
                                    Stop generating
                                </button>
                            </div>
                        )}

                        <div ref={chatEndRef} />
                    </div>

                    {/* Input */}
                    <div style={s.inputRow}>
                        <input
                            style={s.input}
                            value={input}
                            onChange={e => setInput(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder="Ask a question..."
                            disabled={loading}
                        />
                        <button style={s.sendBtn} onClick={handleSend} disabled={loading}>
                            send
                        </button>
                    </div>
                </div>

                {/* ── Right pane: results ── */}
                <div style={s.rightPane}>
                    <div style={s.tableWrap}>
                        {rows.length === 0 ? (
                            <div style={s.empty}>Results will appear here</div>
                        ) : (
                            <table style={s.table}>
                                <thead>
                                    <tr>
                                        {columns.map(col => (
                                            <th key={col} style={s.th}>{col}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {rows.map((row, i) => (
                                        <tr key={i}>
                                            {columns.map(col => (
                                                <td key={col} style={s.td}>
                                                    {row[col] === null || row[col] === undefined
                                                        ? ""
                                                        : String(row[col])}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}
                    </div>
                    <div style={s.rightFooter}>
                        <span style={s.meta}>
                            {rows.length > 0 ? `${rows.length} rows · last run ${lastRun}` : ""}
                        </span>
                        {rows.length > 0 && (
                            <button style={s.exportBtn} onClick={handleExportCSV}>
                                {csvCopied ? "✓ copied!" : "export CSV"}
                            </button>
                        )}
                    </div>
                </div>
            </div>

            {/* ── CSV modal — shown when clipboard API is blocked by PBI sandbox ── */}
            {csvModal && (
                <div style={s.modalOverlay} onClick={() => setCsvModal(null)}>
                    <div style={s.modalCard} onClick={e => e.stopPropagation()}>
                        <div style={s.modalHeader}>
                            <span style={s.modalTitle}>
                                Copy CSV — paste into Excel or Notepad
                            </span>
                            <button style={s.modalClose} onClick={() => setCsvModal(null)}>
                                ×
                            </button>
                        </div>
                        <textarea
                            id="csv-modal-textarea"
                            readOnly
                            value={csvModal}
                            style={s.modalTextarea}
                            onFocus={e => e.currentTarget.select()}
                        />
                        <div style={s.modalFooter}>
                            <button
                                style={s.modalSelectBtn}
                                onClick={() => {
                                    const ta = document.getElementById("csv-modal-textarea") as HTMLTextAreaElement | null;
                                    ta?.select();
                                }}>
                                Select all
                            </button>
                            <button style={s.modalDoneBtn} onClick={() => setCsvModal(null)}>
                                Done
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

// ── Export wrapped in error boundary ──────────────────────────────────────────

export const ChatVisual: React.FC = () => (
    <ErrorBoundary>
        <ChatVisualInner />
    </ErrorBoundary>
);