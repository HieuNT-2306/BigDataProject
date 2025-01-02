import './App.css';
import './index.css';
import Recommend_web from './Recommend_web';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Recommend_web />} />
      </Routes>
    </Router>
  );
}

export default App;
