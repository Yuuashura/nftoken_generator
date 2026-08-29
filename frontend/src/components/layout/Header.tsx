import React from 'react';
import { Globe } from 'lucide-react';

export const AnnouncementBar: React.FC = () => (
  <div className="announcement-bar">
    <span>Cohere AI Infrastructure Protocol &mdash; Netflix NFToken Gateway</span>
    <a href="https://github.com/Yuuashura" target="_blank" rel="noopener" className="text-coral-soft underline font-medium">
      Explore Docs &rarr;
    </a>
  </div>
);

export const Navbar: React.FC = () => {
  return (
    <nav className="navbar">
      <a href="/" className="nav-brand">
        <div className="nav-logo">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 2v20l7-5 7 5V2H4zm12 14.5l-5-3.57-5 3.57V4h10v12.5z" />
          </svg>
        </div>
        <span className="nav-title">Cohere <b>NFToken</b></span>
      </a>
      <div className="nav-links">
        <span className="who">Yuuashura</span>
        <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">
          GitHub
        </a>
        <a href="https://yuuashura.my.id" target="_blank" rel="noopener">
          <Globe className="w-5 h-5" aria-hidden="true" />
          <span className="sr-only">Website</span>
        </a>
        <a href="https://instagram.com/yudis.ashura" target="_blank" rel="noopener">
          Instagram
        </a>
      </div>
    </nav>
  );
};

export const Footer: React.FC = () => (
  <div className="wm">
    <span className="who">Yuuashura</span>
    <a href="https://github.com/Yuuashura" target="_blank" rel="noopener">GitHub: Yuuashura</a>
    <a href="https://yuuashura.my.id" target="_blank" rel="noopener">Web: yuuashura.my.id</a>
    <a href="https://instagram.com/yudis.ashura" target="_blank" rel="noopener">Instagram: yudis.ashura</a>
  </div>
);

export default Navbar;