import type { Capture } from '../model/types.js';
import { accessEvaluator, outcomeKey } from '../access/evaluate.js';
import type { AccessEvaluation, ReachEvaluation, HostAccess, Columns } from '../access/types.js';
export type { ProvenanceState, HostAccess, Columns } from '../access/types.js';
export { evidenceGaps, absentFrom, cancelledAcross } from '../access/evidence.js';
export { edgesBetween } from '../access/traversal.js';

/** A line explaining an answer, in parts for styling: lead, names, plain text. */
export interface ProvenanceLine {
  text: string;
  parts: { text: string; kind: 'lead' | 'name' | 'plain' }[];
}

export interface ProvenanceAnswer extends AccessEvaluation { line: ProvenanceLine }
export interface ReachAnswer { operation: string; state: ReachEvaluation['state']; line: ProvenanceLine; tables: number }

type Part = ProvenanceLine['parts'][number];
const plain = (text: string): Part => ({ text, kind: 'plain' }), name = (text: string): Part => ({ text, kind: 'name' });
const lead = (text: string): Part => ({ text, kind: 'lead' });
const list = (names: string[], separator = ', '): Part[] => names.flatMap((n, i) => [...(i ? [plain(separator)] : []), name(n)]);
const line = (parts: Part[]): ProvenanceLine => ({ text: parts.map(p => p.text).join(''), parts });

const columnParts = (columns: Columns): Part[] => columns.kind === 'only' ? [plain(' · only columns '), ...list(columns.columns)]
  : columns.kind === 'except' ? [plain(' · all columns except '), ...list(columns.columns)] : [];

function describeHost(access: HostAccess, user: string, table: string, operation: string): Part[] {
  const granted = [lead('Granted through'), plain(' '), ...list(access.grantedThrough), plain(' for '), name(operation), ...columnParts(access.columns)];
  switch (access.state) {
    case 'granted': case 'columns': return granted;
    case 'set-role': return [...granted, plain('. '), ...access.inactiveRoles.flatMap((role, i): Part[] => [...(i ? [plain('; ')] : []),
      name(role), plain(' is not active when '), name(user), plain(' logs in; it needs '), name(`SET ROLE ${role}`)])];
    case 'revoked': return [lead('No grant path left:'), plain(' '), ...access.cancelled.flatMap((c, i): Part[] => [...(i ? [plain('; ')] : []),
      name(c.holder), plain(' has a revoke that takes back its '), name(operation), plain(' on '), name(table)])];
    default: return [lead('No grant path:'), plain(' no role or privilege of '), name(user), plain(' covers '), name(operation), plain(' on '), name(table)];
  }
}

const outcome = (access: HostAccess) => access.state + (access.columns.kind === 'only' || access.columns.kind === 'except'
  ? ` (${access.columns.kind === 'only' ? 'only' : 'except'} ${access.columns.columns.join(', ')})` : '')
  + (access.state === 'set-role' ? ` (SET ROLE ${access.inactiveRoles.join(', ')})` : '');

/** Public presentation wrappers evaluate raw capture evidence, independently of display filters. */
export function answerFor(capture: Capture, user: string, table: string, operation: string): ProvenanceAnswer {
  return describeAccess(accessEvaluator(capture).answer(user, table, operation));
}

export function describeAccess(answer: AccessEvaluation): ProvenanceAnswer {
  const { hosts, gaps, userName, tableName, operation, state } = answer;
  const result = (parts: Part[]): ProvenanceAnswer => ({ ...answer, line: line(parts) });
  const groups = [...new Set(hosts.map(outcomeKey))];
  if (groups.length > 1) {
    return result([lead('Differs by host:'), plain(' '), ...groups.flatMap((group, i): Part[] => {
      const members = hosts.filter(h => outcomeKey(h) === group);
      return [...(i ? [plain('; ')] : []), name(outcome(members[0])), plain(' on '), ...list(members.map(h => h.host))];
    }), ...(gaps.length ? [plain(' · collection incomplete: '), ...list(gaps)] : [])]);
  }
  const parts = hosts[0] ? describeHost(hosts[0], userName, tableName, operation)
    : [lead('Not found:'), plain(' '), name(tableName), plain(' is not among the collected tables')];
  if (state === 'incomplete') return result([lead('Incomplete:'), plain(' '), ...list(gaps), plain('. '), ...parts]);
  return result(gaps.length ? [...parts, plain(' · collection incomplete: '), ...list(gaps)] : parts);
}

export function reachFor(capture: Capture, user: string, operation: string): ReachAnswer {
  return describeReach(accessEvaluator(capture).reach(user, operation));
}

export function describeReach(evaluation: ReachEvaluation): ReachAnswer {
  const { operation, state, tables, userName, login, loginSome, later, laterSome, needs, gaps } = evaluation;
  const byRoles = [...new Set(needs.map(n => n.roles.join(', ')))];
  const setRole = byRoles.length === 1 ? `SET ROLE ${byRoles[0]}`
    : byRoles.map(roles => `SET ROLE ${roles} on ${[...new Set(needs.filter(n => n.roles.join(', ') === roles).map(n => n.host))].sort().join(', ')}`).join('; ');
  const tablesText = (n: number) => `${n} table${n === 1 ? '' : 's'}`;
  const gapNote: Part[] = gaps.length ? [plain(' · collection incomplete: '), ...list(gaps)] : [];
  const laterNote: Part[] = later ? [plain(` · ${tablesText(later)} after `), name(setRole), ...(laterSome ? [plain(`, ${laterSome} of them on some hosts only`)] : [])] : [];
  const answer = (parts: Part[]): ReachAnswer => ({ operation, state, tables, line: line(parts) });
  if (login) return answer([lead(`Reaches ${tablesText(login)}`), plain(' for '), name(operation),
    ...(loginSome ? [plain(` · ${tablesText(loginSome)} on some hosts only`)] : []), ...laterNote, ...gapNote]);
  if (loginSome) return answer([lead(`Reaches ${tablesText(loginSome)} on some hosts only`), plain(' for '), name(operation), ...laterNote, ...gapNote]);
  if (later) return answer([lead(`Reaches ${tablesText(later)}`), plain(' for '), name(operation),
    plain(' only after '), name(setRole), ...(laterSome ? [plain(`, ${laterSome} of them on some hosts only`)] : []), ...gapNote]);
  const none: Part[] = [lead('No privilege'), plain(' of '), name(userName), plain(' includes '), name(operation)];
  return answer(state === 'incomplete' ? [lead('Incomplete:'), plain(' '), ...list(gaps), plain('. '), ...none] : none);
}
