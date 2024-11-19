// frontend/cryptodashboard/src/components/TradingTable.js

import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    Paper,
    CircularProgress,
    Typography,
} from '@mui/material';

const TradingTable = () => {
    const [tradingData, setTradingData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchTradingData();
        const interval = setInterval(fetchTradingData, 60000); // Fetch every 1 minute

        return () => clearInterval(interval); // Cleanup on unmount
    }, []);

    const fetchTradingData = async () => {
        try {
            setLoading(true);
            const response = await axios.get('http://localhost:8000/api/trading'); // Replace <EC2_PUBLIC_IP> with your actual IP
            setTradingData(response.data);
            setLoading(false);
        } catch (err) {
            console.error(err);
            setError('Failed to fetch trading data.');
            setLoading(false);
        }
    };

    if (loading) {
        return <CircularProgress />;
    }

    if (error) {
        return <Typography color="error">{error}</Typography>;
    }

    return (
        <TableContainer component={Paper}>
            <Table>
                <TableHead>
                    <TableRow>
                        <TableCell>Coin Name</TableCell>
                        <TableCell>Purchase Price</TableCell>
                        <TableCell>Last Price</TableCell>
                        <TableCell>Percentage Increase (%)</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {tradingData.map((row) => (
                        <TableRow key={row.symbol}>
                            <TableCell>{row.symbol}</TableCell>
                            <TableCell>
                                {row.purchasePrice !== null && row.purchasePrice !== 0
                                    ? row.purchasePrice.toFixed(2)
                                    : 'N/A'}
                            </TableCell>
                            <TableCell>{row.lastPrice.toFixed(2)}</TableCell>
                            <TableCell>{row.percentage_increase.toFixed(2)}</TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
        </TableContainer>
    );
};

export default TradingTable;
