import { keyframes } from '@emotion/css';

export const pulseAnimation = keyframes`0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }`;
export const bandPulseAnimation = keyframes`0%, 100% { opacity: 0.7; }
    50% { opacity: 0.45; }`;

export const spinAnimation = keyframes`to { transform: rotate(360deg); }`;

export const pulseDotAnimation = keyframes`0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.75); }`;
