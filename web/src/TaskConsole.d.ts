import type { ReactElement } from 'react';

export default function TaskConsole(props: {
  route?: { page: string; task: string | null; clusterId?: string | null };
  navigatePage?: (page: string, id?: string) => void;
  clearClusterFilter?: () => void;
  onCounts?: (tasks: number, alerts: number) => void;
}): ReactElement;
