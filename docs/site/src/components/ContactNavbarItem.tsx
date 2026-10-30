import React, { useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { contactEmail } from './contactEmail';

const ISSUES_URL = 'https://github.com/dmkskd/tracehouse/issues';

function ContactOptions({ email, optionClassName }: { email: string; optionClassName: string }): ReactNode {
  return (
    <>
      <a href={`mailto:${email}`} className={optionClassName}>
        <span className="contact-option-label">Email</span>
        <span className="contact-email">{email}</span>
      </a>
      <a href={ISSUES_URL} target="_blank" rel="noopener noreferrer" className={optionClassName}>
        <span className="contact-option-label">GitHub</span>
        Raise an issue
      </a>
    </>
  );
}

/**
 * Navbar "Contact" item. Shows email + GitHub issues in place (popover on desktop, inline
 * in the mobile menu) so the reader stays on the page they were reading.
 */
export default function ContactNavbarItem({ mobile = false }: { mobile?: boolean }): ReactNode {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open || mobile) return;
    const onMouseDown = (e: MouseEvent) => {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open, mobile]);

  const email = open ? contactEmail() : null;

  if (mobile) {
    return (
      <li className="menu__list-item">
        <button type="button" className="menu__link contact-navbar-button" onClick={() => setOpen(o => !o)}>
          Contact
        </button>
        {email && (
          <div className="contact-mobile-options">
            <ContactOptions email={email} optionClassName="menu__link" />
          </div>
        )}
      </li>
    );
  }

  return (
    <div ref={wrapperRef} className="navbar__item contact-navbar-item">
      <button
        type="button"
        className="navbar__link contact-navbar-button"
        aria-expanded={open}
        onClick={() => setOpen(o => !o)}
      >
        Contact
      </button>
      {email && (
        <div className="demo-dropdown contact-popover" role="dialog" aria-label="Contact">
          <div className="contact-popover-title">Questions or feedback?</div>
          <ContactOptions email={email} optionClassName="demo-dropdown-option" />
        </div>
      )}
    </div>
  );
}
