import { HashRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Layout } from './components/Layout';
import { QueryMonitor } from './pages/QueryMonitor';
import { DatabaseExplorer } from './pages/DatabaseExplorer';
import { MergeTracker } from './pages/MergeTracker';
import { Overview } from './pages/Overview';
import { TimeTravelPage } from './pages/TimeTravelPage';
import { EngineInternals } from './pages/EngineInternals';
import { ClusterOverview } from './pages/ClusterOverview';
import { Analytics } from './pages/Analytics';
import { Replication } from './pages/Replication';
import { ClickHouseProvider } from './providers/ClickHouseProvider';
import { ThemeProvider } from './providers/ThemeProvider';
import { RefreshConfigContext, DEFAULT_REFRESH_CONFIG } from '@tracehouse/ui-shared';
import './styles/themes.css';

function App() {
  return (
    <HashRouter>
      <ThemeProvider>
        <ClickHouseProvider>
          <RefreshConfigContext.Provider value={DEFAULT_REFRESH_CONFIG}>
            <Layout>
              <Routes>
                <Route path="/" element={<Navigate to="/overview" replace />} />
                <Route path="/overview" element={<Overview />} />
                <Route path="/metrics" element={<Navigate to="/overview" replace />} />
                <Route path="/queries" element={<QueryMonitor />} />
                <Route path="/databases" element={<DatabaseExplorer />} />
                <Route path="/merges" element={<MergeTracker />} />
                <Route path="/timetravel" element={<TimeTravelPage />} />
                <Route path="/live-view" element={<Navigate to="/overview" replace />} />
                <Route path="/engine-internals" element={<EngineInternals />} />
                <Route path="/cluster" element={<ClusterOverview />} />
                <Route path="/replication" element={<Replication />} />
                <Route path="/analytics" element={<Analytics />} />
              </Routes>
            </Layout>
          </RefreshConfigContext.Provider>
        </ClickHouseProvider>
      </ThemeProvider>
    </HashRouter>
  );
}

export default App;
