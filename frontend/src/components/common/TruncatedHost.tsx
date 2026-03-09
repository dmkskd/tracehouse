import React from 'react';
import { truncateHostname } from '@tracehouse/core';

/**
 * Renders a hostname truncated for compact display, with a native
 * tooltip showing the full name on hover.
 */
export const TruncatedHost: React.FC<{
  name: string;
  maxLen?: number;
}> = ({ name, maxLen }) => {
  const short = truncateHostname(name, maxLen);
  const needsTruncation = short !== name;
  return (
    <span title={needsTruncation ? name : undefined} style={needsTruncation ? { cursor: 'default' } : undefined}>
      {short}
    </span>
  );
};
