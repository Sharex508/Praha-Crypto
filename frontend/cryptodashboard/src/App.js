// frontend/cryptodashboard/src/App.js

import React from 'react';
import { Container, Typography } from '@mui/material';
import TradingTable from './components/TradingTable';

function App() {
    return (
        <Container>
            <Typography variant="h4" gutterBottom style={{ marginTop: '20px' }}>
                Cryptocurrency Trading Data
            </Typography>
            <TradingTable />
        </Container>
    );
}

export default App;
