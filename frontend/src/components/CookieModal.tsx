import React, { useState } from 'react';

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  metaInfo?: string;
  netscape?: string;
  json?: string;
  raw?: string;
}

export const CookieModal: React.FC<ModalProps> = ({
  isOpen,
  onClose,
  title,
  metaInfo,
  netscape = '',
  json = '',
  raw = '',
}) => {
  const [activeFmt, setActiveFmt] = useState<'raw' | 'netscape' | 'json'>('raw');
  const [copied, setCopied] = useState(false);

  if (!isOpen) return null;

  const currentText = activeFmt === 'netscape' ? netscape : activeFmt === 'json' ? json : raw;

  const handleCopy = () => {
    if (navigator.clipboard && currentText) {
      navigator.clipboard.writeText(currentText).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      });
    }
  };

  return (
    <div className="modal-overlay">
      <div className="modal-card">
        <div className="modal-head">
          <span>{title}</span>
          <button className="btn-compact" onClick={onClose}>
            ✕
          </button>
        </div>
        {metaInfo && <div className="modal-meta">{metaInfo}</div>}
        <div className="modal-tabs">
          <button
            className={`cm-tab ${activeFmt === 'raw' ? 'on' : ''}`}
            onClick={() => setActiveFmt('raw')}
          >
            Raw
          </button>
          <button
            className={`cm-tab ${activeFmt === 'netscape' ? 'on' : ''}`}
            onClick={() => setActiveFmt('netscape')}
          >
            Netscape
          </button>
          <button
            className={`cm-tab ${activeFmt === 'json' ? 'on' : ''}`}
            onClick={() => setActiveFmt('json')}
          >
            JSON
          </button>
        </div>
        <textarea value={currentText} readOnly spellCheck={false} />
        <div className="modal-actions">
          <button className="btn-pill-primary" onClick={handleCopy}>
            {copied ? 'Tersalin ✓' : '📋 Salin Cookie'}
          </button>
        </div>
      </div>
    </div>
  );
};
