import { keyframes } from '@emotion/css';

export const arenaScanAnimation = keyframes`0% { top: -2px; } 100% { top: 100%; }`;
export const arenaFadeInAnimation = keyframes`0% { opacity: 0; transform: translateY(8px); } 100% { opacity: 1; transform: translateY(0); }`;
export const arenaGlowPulseAnimation = keyframes`0%,100% { opacity: 0.6; } 50% { opacity: 1; }`;
export const arenaBarFillAnimation = keyframes`0% { width: 0%; } 100% { width: var(--bar-pct); }`;
export const arenaTypingAnimation = keyframes`0% { width: 0; } 100% { width: 100%; }`;
export const arenaLogPulseAnimation = keyframes`0%,100% { background-position: 0% 0%; opacity: 0.7; } 50% { background-position: 100% 0%; opacity: 1; }`;
