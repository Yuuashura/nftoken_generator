import { useState } from 'react';

function App() {
  const [activeTab, setActiveTab] = useState<'single' | 'bulk' | 'checker'>('single');

  return (
    <div className="min-h-screen bg-white">
      <div className="announcement-bar">
        <span>Cohere AI Infrastructure Protocol &mdash; Netflix NFToken Gateway</span>
        <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">
          Explore Docs &rarr;
        </a>
      </div>

      <nav className="navbar">
        <a href="/" className="nav-brand">
          <div className="nav-logo">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M4 2v20l7-5 7 5V2H4zm12 14.5l-5-3.57-5 3.57V4h10v12.5z" />
            </svg>
          </div>
          <span className="nav-title">Cohere <b>NFToken</b></span>
        </a>
        <nav className="nav-links">
          <span className="who">Yuuashura</span>
          <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">
            GitHub
          </a>
          <a href="https://yuuashura.my.id" target="_blank" rel="noopener">
            Website
          </a>
          <a href="https://instagram.com/yudis.ashura" target="_blank" rel="noopener">
            Instagram
          </a>
        </nav>
      </nav>

      <div className="wrap">
        <div className="hero-section">
          <span className="hero-tag">Enterprise Token Engine</span>
          <h1 className="hero-title">Automated <b>NFToken</b> Link Generation & Cookie Verification</h1>
          <p className="hero-sub">Enterprise-grade authentication proxy. Parse Netscape, JSON, or raw cookies, verify live membership tiers, and issue instant NFToken login URLs.</p>
        </div>

        <div className="tabs-container">
          <button className="tab" onClick={() => setActiveTab('single')}>Single Generator</button>
          <button className="tab" onClick={() => setActiveTab('bulk')}>Bulk Generator</button>
          <button className="tab" onClick={() => setActiveTab('checker')}>Live Checker & Generator</button>
        </div>

        <div className="panel" hidden={activeTab !== 'single'}>
          <div className="cohere-card">
            <div className="field-group">
              <label className="mono-label">Input Cookie Payload (Netscape / JSON / Raw)</label>
              <textarea
                id="taSingle"
                spellCheck={false}
                placeholder={`
{
    "NetflixId": "v%3D3%26ct%3D...",
    "SecureNetflixId": "v%3D3%26mac%3D...",
    "nfvdid": "..."
}

atau raw:
NetflixId=v%3D3%26ct%3D...; SecureNetflixId=...; nfvdid=...`}
                rows={8}
              />
            </div>

            <div className="optional-meta">
              <div className="meta-field">
                <span className="field-label">Plan Tier (Optional Manual Input)</span>
                <input type="text" id="inputPlan" placeholder="e.g. Basic, Standard, Premium 4K" />
              </div>
              <div className="meta-field">
                <span className="field-label">Billing Date (Optional Manual Input)</span>
                <input type="date" id="inputBilling" />
              </div>
            </div>

            <div className="btn-row">
              <button className="btn-pill-primary" id="btnSingle">Generate NFToken Links</button>
              <button className="btn-pill-outline" id="fileSingle">Generate from My File</button>
              <button className="btn-pill-outline" id="exSingle">Load Example</button>
              <button className="btn-pill-outline" id="clearSingle">Clear</button>
            </div>

            <div className="err-msg hidden" id="singleErr"></div>

            <div className="result-card-box hidden" id="singleResult">
              <div className="result-heading">Issued NFToken Authentication Links</div>
              <div id="singleLinks"></div>
              <div className="meta-info-grid">
                <div>Token Expiration: <span id="singleExp"></span></div>
              </div>
            </div>
          </div>
        </div>

        <div className="panel" hidden>
          <div className="cohere-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
              <label className="mono-label" style={{ marginBottom: 0 }}>Bulk Cookie Payload (Paste or Upload .txt / .zip)</label>
              <div>
                <input type="file" id="fileBulkInput" accept=".txt,.zip" style={{ display: 'none' }} />
                <button className="btn-pill-outline" id="btnUploadBulk" style={{ padding: '6px 16px', fontSize: '12px' }}>📁 Upload File (.txt / .zip)</button>
              </div>
            </div>
            <div id="fileBulkBadge" className="file-badge hidden"></div>

            <textarea
              id="taBulk"
              spellCheck={false}
              placeholder={`
# [1] account1@domain.com
.netflix.com  TRUE  /  TRUE  1779826982  NetflixId  v%3D3%26ct%3D...
.netflix.com  TRUE  /  TRUE  1779826982  SecureNetflixId  v%3D3%26mac%3D...

# [2] account2@domain.com
NetflixId=v%3D3%26ct%3D...; SecureNetflixId=v%3D3%26mac%3D...`}
              rows={10}
            />

            <div className="btn-row">
              <button className="btn-pill-primary" id="startBulk">Process All Accounts</button>
              <button className="btn-pill-outline" id="exBulk">Load Example</button>
              <button className="btn-pill-outline" id="clearBulk">Clear</button>
              <button className="btn-pill-outline" hidden id="dlBulk">Download Results (.txt)</button>
            </div>

            <div className="err-msg hidden" id="bulkErr"></div>
            <div style={{ fontSize: '14px', color: 'var(--slate)', marginTop: '12px' }} hidden id="bulkHint"></div>
            <div className="progress-bar-wrap hidden" id="barWrap"><div className="progress-bar-fill" id="barFill" /></div>
            <div style={{ fontSize: '14px', fontFamily: 'var(--font-mono)', marginBottom: '12px' }} hidden id="bulkSum"></div>
            <button className="btn-pill-outline hidden" id="stopBulk">Halt Execution</button>

            <div className="output-list-box hidden" id="bulkList" style={{ marginTop: '16px' }}></div>
          </div>
        </div>

        <div className="panel" hidden>
          <div className="cohere-card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px', flexWrap: 'wrap', gap: '8px' }}>
              <label className="mono-label" style={{ marginBottom: 0 }}>Live Account Verification Engine</label>
              <div>
                <input type="file" id="fileCheckerInput" accept=".txt,.zip" style={{ display: 'none' }} />
                <button className="btn-pill-outline" id="btnUploadChecker" style={{ padding: '6px 16px', fontSize: '12px' }}>📁 Upload File (.txt / .zip)</button>
              </div>
            </div>
            <div id="fileCheckerBadge" className="file-badge hidden"></div>

            <textarea id="taChecker" spellCheck={false} placeholder={`
# [1] account1@domain.com
.netflix.com  TRUE  /  TRUE  1779826982  NetflixId  v%3D3%26ct%3D...
.netflix.com  TRUE  /  TRUE  1779826982  SecureNetflixId  v%3D3%26mac%3D...

# [2] account2@domain.com
NetflixId=v%3D3%26ct%3D...; SecureNetflixId=v%3D3%26mac%3D...`}
              rows={10}
            />

            <div className="btn-row">
              <button className="btn-pill-primary" id="startChecker">🔍 Verify Live Memberships</button>
              <button className="btn-pill-outline" id="exChecker">Load Example</button>
              <button className="btn-pill-outline" id="clearChecker">Clear</button>
              <button className="btn-pill-outline" hidden id="stopChecker">Stop Check</button>
            </div>

            <div className="err-msg hidden" id="chkErr"></div>
            <div style={{ fontSize: '14px', color: 'var(--slate)', marginTop: '12px' }} hidden id="chkHint"></div>
            <div className="progress-bar-wrap hidden" id="chkBarWrap"><div className="progress-bar-fill" id="chkBarFill" /></div>
            <div style={{ fontSize: '14px', fontFamily: 'var(--font-mono)', marginBottom: '12px' }} hidden id="chkSum"></div>

            <div className="hidden" id="chkResultsArea" style={{ marginTop: '24px' }}>
              <div className="checker-action-bar">
                <button className="btn-compact" id="chkSelectAll">Select All Live</button>
                <button className="btn-compact" id="chkDeselectAll">Deselect All</button>
                <span style={{ flex: 1 }}></span>
                <button className="btn-pill-primary" id="btnGenSelected" style={{ padding: '8px 20px', fontSize: '13px' }}>🚀 Issue Tokens for Selected (<span id="selectedCount">0</span>)</button>
              </div>

              <div className="table-responsive">
                <table className="cohere-table">
                  <thead>
                    <tr>
                      <th style={{ width: '40px' }}><input type="checkbox" id="chkMaster" /></th>
                      <th style={{ width: '80px' }}>Status</th>
                      <th>Account Label / Email</th>
                      <th>Detected Plan</th>
                      <th>Next Billing Date</th>
                      <th>Region</th>
                      <th style={{ width: '100px' }}>Action</th>
                    </tr>
                  </thead>
                  <tbody id="chkTableBody"></tbody>
                </table>
              </div>
            </div>

            <div className="result-card-box hidden" id="genSelectedResult">
              <div className="result-heading">Generated NFToken Links for Active Accounts</div>
              <div id="genSelectedLinks"></div>
            </div>
          </div>
        </div>
      </div>

      <footer className="wm">
        <span className="who">Yuuashura</span>
        <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">GitHub: Yuuashura</a>
        <a href="https://yuuashura.my.id" target="_blank" rel="noopener">Web: yuuashura.my.id</a>
        <a href="https://instagram.com/yudis.ashura" target="_blank" rel="noopener">Instagram: yudis.ashura</a>
      </footer>
    </div>
  );
};

export default App;