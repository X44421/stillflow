import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@patternfly/react-core/dist/styles/base.css';
import '@patternfly/patternfly/patternfly-charts.css';
import '@patternfly/react-topology/dist/esm/css/topology-controlbar.css';
import '@patternfly/react-topology/dist/esm/css/topology-side-bar.css';
import '@patternfly/react-topology/dist/esm/css/topology-view.css';
import '@patternfly/react-topology/dist/esm/css/topology-components.css';
import '@patternfly/react-topology/dist/esm/css/topology-pipelines.css';
import './index.css';
import App from './App';

document.documentElement.classList.add('pf-v6-theme-dark');

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
