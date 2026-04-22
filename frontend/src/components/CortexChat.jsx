import { useState, useRef, useEffect, useCallback } from 'react';
import { sendChatMessage } from '../api/client';
import { downloadJson, printText } from '../utils/export';

const MODELS = [
  { id: 'llama3.1-8b', label: 'Llama 3.1 8B' },
  { id: 'snowflake-arctic', label: 'Snowflake Arctic' },
];

const styles = {
  container: {
    display: 'flex',
    flexDirection: 'column',
    height: 'calc(100vh - 90px)',
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    padding: '10px 20px',
    background: '#16213e',
    borderBottom: '1px solid #0f3460',
  },
  select: {
    padding: '6px 10px',
    background: '#1a1a2e',
    border: '1px solid #0f3460',
    borderRadius: 4,
    color: '#e0e0e0',
    fontSize: 13,
    outline: 'none',
  },
  modelLabel: {
    fontSize: 12,
    color: '#8892b0',
  },
  messages: {
    flex: 1,
    overflowY: 'auto',
    padding: 20,
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
  },
  msg: (isUser) => ({
    maxWidth: '70%',
    alignSelf: isUser ? 'flex-end' : 'flex-start',
    background: isUser ? '#0f3460' : '#16213e',
    border: isUser ? '1px solid #e94560' : '1px solid #0f3460',
    borderRadius: isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
    padding: '10px 14px',
    fontSize: 13,
    lineHeight: 1.5,
    color: '#e0e0e0',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  }),
  msgLabel: (isUser) => ({
    fontSize: 10,
    color: '#8892b0',
    marginBottom: 2,
    textAlign: isUser ? 'right' : 'left',
  }),
  thinking: {
    alignSelf: 'flex-start',
    color: '#8892b0',
    fontSize: 13,
    fontStyle: 'italic',
    padding: '10px 14px',
  },
  inputArea: {
    display: 'flex',
    gap: 8,
    padding: '12px 20px',
    background: '#16213e',
    borderTop: '1px solid #0f3460',
  },
  input: {
    flex: 1,
    padding: '10px 14px',
    background: '#1a1a2e',
    border: '1px solid #0f3460',
    borderRadius: 8,
    color: '#e0e0e0',
    fontSize: 13,
    outline: 'none',
  },
  sendBtn: {
    padding: '10px 20px',
    background: '#e94560',
    color: '#fff',
    border: 'none',
    borderRadius: 8,
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 600,
  },
  sendBtnDisabled: {
    padding: '10px 20px',
    background: '#3a3a5c',
    color: '#8892b0',
    border: 'none',
    borderRadius: 8,
    cursor: 'not-allowed',
    fontSize: 13,
    fontWeight: 600,
  },
  empty: {
    textAlign: 'center',
    color: '#8892b0',
    padding: 40,
    fontSize: 14,
  },
};

export default function CortexChat() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [model, setModel] = useState(MODELS[0].id);
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, loading]);

  const send = useCallback(async () => {
    const text = input.trim();
    if (!text || loading) return;
    setInput('');
    setMessages((prev) => [...prev, { role: 'user', content: text }]);
    setLoading(true);
    try {
      const data = await sendChatMessage(text, model);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: data.response || data.message || 'No response.' },
      ]);
    } catch (err) {
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `Error: ${err.message}` },
      ]);
    } finally {
      setLoading(false);
    }
  }, [input, model, loading]);

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  const exportTranscript = useCallback(() => {
    const text = messages.map((m) =>
      `${m.role === 'user' ? 'You' : 'Cortex AI'}:\n${m.content}`
    ).join('\n\n---\n\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'chat_transcript.txt';
    a.click();
    URL.revokeObjectURL(url);
  }, [messages]);

  const chatBtnStyle = {
    padding: '5px 12px',
    background: '#0f3460',
    color: '#e0e0e0',
    border: '1px solid #e94560',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 12,
  };

  return (
    <div style={styles.container}>
      <div style={styles.toolbar}>
        <span style={styles.modelLabel}>Model:</span>
        <select
          style={styles.select}
          value={model}
          onChange={(e) => setModel(e.target.value)}
        >
          {MODELS.map((m) => (
            <option key={m.id} value={m.id}>{m.label}</option>
          ))}
        </select>
        {messages.length > 0 && (
          <>
            <button style={chatBtnStyle} onClick={exportTranscript}>
              TXT
            </button>
            <button style={chatBtnStyle} onClick={() => downloadJson(messages, 'chat_transcript.json')}>
              JSON
            </button>
            <button style={chatBtnStyle} onClick={() => {
              const text = messages.map((m) =>
                `${m.role === 'user' ? 'You' : 'Cortex AI'}:\n${m.content}`
              ).join('\n\n---\n\n');
              printText(text, 'Cortex AI Chat Transcript');
            }}>
              Print
            </button>
          </>
        )}
      </div>

      <div style={styles.messages} ref={scrollRef}>
        {messages.length === 0 && (
          <div style={styles.empty}>
            Start a conversation with Cortex AI. Ask questions about your data, SQL help, or geospatial analysis.
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i}>
            <div style={styles.msgLabel(msg.role === 'user')}>
              {msg.role === 'user' ? 'You' : 'Cortex AI'}
            </div>
            <div style={styles.msg(msg.role === 'user')}>{msg.content}</div>
          </div>
        ))}
        {loading && <div style={styles.thinking}>Thinking...</div>}
      </div>

      <div style={styles.inputArea}>
        <input
          style={styles.input}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask Cortex AI anything..."
          disabled={loading}
        />
        <button
          style={loading ? styles.sendBtnDisabled : styles.sendBtn}
          onClick={send}
          disabled={loading}
        >
          Send
        </button>
      </div>
    </div>
  );
}
