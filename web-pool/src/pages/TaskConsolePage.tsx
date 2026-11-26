import { Navigate, useLocation, useNavigate, useOutletContext } from 'react-router-dom';
import TaskConsole from '../../../web/src/TaskConsole.jsx';
import { consolePage, consolePath, ROUTES } from '../routes/config';
import type { MainLayoutOutletContext } from '../layouts/MainLayout';

export default function TaskConsolePage() {
  const { search } = useLocation();
  const navigate = useNavigate();
  const { updateCounts } = useOutletContext<MainLayoutOutletContext>();
  const page = consolePage(search);
  // The former task-side node page duplicated node administration; nodes live in the pool menu.
  if (page === 'nodes') return <Navigate to={ROUTES.nodes} replace />;
  const params = new URLSearchParams(search);
  const task = page === 'detail' ? params.get('task') : null;
  const clusterId = page === 'tasks' || page === 'detail' ? params.get('cluster_id') : null;
  const navigatePage = (next: string, id?: string) => {
    const path = consolePath(next, id);
    const scope = clusterId && (next === 'tasks' || next === 'detail')
      ? '&' + new URLSearchParams({ cluster_id: clusterId }) : '';
    navigate(path + scope);
  };
  return (
    <TaskConsole
      route={{ page, task, clusterId }}
      navigatePage={navigatePage}
      clearClusterFilter={() => navigate(consolePath('tasks'))}
      onCounts={updateCounts}
    />
  );
}
