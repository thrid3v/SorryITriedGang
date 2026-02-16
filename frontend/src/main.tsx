import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";

// Add global error handler
window.addEventListener('error', (e) => {
  console.error('🔴 GLOBAL ERROR:', e.error);
  console.error('Message:', e.message);
  console.error('Filename:', e.filename);
  console.error('Line:', e.lineno, 'Col:', e.colno);
});

window.addEventListener('unhandledrejection', (e) => {
  console.error('🔴 UNHANDLED PROMISE REJECTION:', e.reason);
});

console.log('✅ main.tsx loaded');

try {
  const rootElement = document.getElementById("root");
  if (!rootElement) {
    throw new Error('Root element not found!');
  }
  console.log('✅ Root element found');
  
  createRoot(rootElement).render(<App />);
  console.log('✅ App rendered');
} catch (error) {
  console.error('🔴 RENDER ERROR:', error);
  document.body.innerHTML = `
    <div style="padding: 20px; background: #ff0000; color: white; font-family: monospace;">
      <h1>React App Failed to Load</h1>
      <pre>${error}</pre>
      <p>Check browser console for details</p>
    </div>
  `;
}
