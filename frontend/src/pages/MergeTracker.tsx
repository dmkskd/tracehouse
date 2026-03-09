/**
 * MergeTracker Page - Main page for merge tracking
 * 
 * This page wraps the MergeTrackerView component and provides the main
 * entry point for the merge tracking feature.
 * 
 */

import React from 'react';
import { MergeTrackerView } from '../components/merge/MergeTracker';

export const MergeTracker: React.FC = () => {
  return <MergeTrackerView />;
};

export default MergeTracker;
