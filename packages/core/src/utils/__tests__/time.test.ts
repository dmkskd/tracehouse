import { describe, it, expect } from 'vitest';
import { parseTimeValue } from '../time.js';

describe('parseTimeValue', { tags: ['storage'] }, () => {
    it('parses standard ClickHouse timestamp successfully', () => {
        // Standard format returned by HTTP ClickHouse adapter
        const result = parseTimeValue('2026-03-04 14:02:28.000000');
        expect(result.timeMs).not.toBeNaN();
        expect(result.timeMs).toBe(new Date('2026-03-04T14:02:28.000Z').getTime());
        // Mapped directly to ISO format
        expect(result.timeStr).toBe(new Date('2026-03-04T14:02:28.000Z').toISOString());
    });

    it('parses Grafana-formatted RFC2822 timestamps successfully', () => {
        // Standard Grafana DataFrame string response under certain environments
        const input = 'Fri, 27 Feb 2026 20:24:42';
        const result = parseTimeValue(input);
        expect(result.timeMs).not.toBeNaN();
        // This value evaluates successfully relative to local time, 
        // ensuring Date.parse works without throwing NaN
        expect(result.timeMs).toBe(Date.parse(input));
        expect(result.timeStr).toBe(new Date(Date.parse(input)).toISOString());
    });

    it('parses integer epoch values successfully', () => {
        const epoch = 1700000000000;
        const result = parseTimeValue(epoch);
        expect(result.timeMs).toBe(epoch);
        expect(result.timeStr).toBe(new Date(epoch).toISOString());
    });

    it('parses string epoch values as fallback', () => {
        const epochStr = "1700000000000";
        const result = parseTimeValue(epochStr);
        expect(result.timeMs).toBe(1700000000000);
        expect(result.timeStr).toBe(new Date(1700000000000).toISOString());
    });

    it('parses Date object instances correctly', () => {
        const now = new Date();
        const result = parseTimeValue(now);
        expect(result.timeMs).toBe(now.getTime());
        expect(result.timeStr).toBe(now.toISOString());
    });

    it('handles invalid inputs gracefully by returning 0 epoch', () => {
        const result = parseTimeValue('invalid string');
        expect(result.timeMs).toBe(0);
        expect(result.timeStr).toBe(new Date(0).toISOString());
    });
});
