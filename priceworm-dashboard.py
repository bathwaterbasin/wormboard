#!/usr/bin/env python3
"""
Crypto Trading Data Fetcher
Integrates CoinGecko, Binance, and Bybit APIs for comprehensive trading data
Supports 6:43 AM EST reference price tracking and sentiment scoring
"""

import os
import json
import time
import asyncio
import aiohttp
import websockets
import schedule
from datetime import datetime, timezone
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class PriceData:
    price: float
    change_24h: float
    volume: float
    market_cap: float
    timestamp: str

@dataclass
class FundingData:
    binance: Optional[float] = None
    bybit: Optional[float] = None
    okx: Optional[float] = None
    average: Optional[float] = None

@dataclass
class LiquidationEvent:
    symbol: str
    side: str  # 'SELL' = long liquidation, 'BUY' = short liquidation
    quantity: float
    price: float
    value: float
    time: str
    exchange: str

@dataclass
class MarketData:
    bitcoin: PriceData
    ethereum: PriceData
    funding_rates: Dict[str, FundingData]
    liquidations: Dict[str, List[LiquidationEvent]]
    sentiment_score: int
    reference_643am: Dict[str, float]
    timestamp: str

class CryptoDataFetcher:
    def __init__(self):
        self.base_urls = {
            'coingecko': 'https://api.coingecko.com/api/v3',
            'binance': 'https://fapi.binance.com/fapi/v1',
            'bybit': 'https://api.bybit.com/v2/public',
            'okx': 'https://www.okx.com/api/v5/public'
        }
        
        # Store reference prices from 6:43 AM EST
        self.reference_643am = {
            'bitcoin': None,
            'ethereum': None,
            'timestamp': None
        }
        
        # Store recent liquidations
        self.recent_liquidations = {
            'bitcoin': [],
            'ethereum': []
        }
        
        # Load stored reference prices
        self.load_reference_prices()
        
        print("🔧 CryptoDataFetcher initialized")

    async def get_price_data(self) -> Optional[Dict[str, PriceData]]:
        """Fetch real-time price data from CoinGecko"""
        try:
            url = f"{self.base_urls['coingecko']}/simple/price"
            params = {
                'ids': 'bitcoin,ethereum',
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
                'include_24hr_vol': 'true',
                'include_market_cap': 'true'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    data = await response.json()
            
            price_data = {
                'bitcoin': PriceData(
                    price=data['bitcoin']['usd'],
                    change_24h=data['bitcoin']['usd_24h_change'],
                    volume=data['bitcoin']['usd_24h_vol'],
                    market_cap=data['bitcoin']['usd_market_cap'],
                    timestamp=datetime.now(timezone.utc).isoformat()
                ),
                'ethereum': PriceData(
                    price=data['ethereum']['usd'],
                    change_24h=data['ethereum']['usd_24h_change'],
                    volume=data['ethereum']['usd_24h_vol'],
                    market_cap=data['ethereum']['usd_market_cap'],
                    timestamp=datetime.now(timezone.utc).isoformat()
                )
            }
            
            print("✅ Price data fetched successfully")
            return price_data
            
        except Exception as e:
            print(f"❌ Error fetching price data: {e}")
            return None

    async def get_funding_rates(self) -> Optional[Dict[str, FundingData]]:
        """Fetch funding rates from multiple exchanges"""
        try:
            funding_data = {
                'bitcoin': FundingData(),
                'ethereum': FundingData()
            }
            
            async with aiohttp.ClientSession() as session:
                # Binance funding rates
                try:
                    btc_url = f"{self.base_urls['binance']}/fundingRate?symbol=BTCUSDT&limit=1"
                    eth_url = f"{self.base_urls['binance']}/fundingRate?symbol=ETHUSDT&limit=1"
                    
                    async with session.get(btc_url) as response:
                        btc_data = await response.json()
                        if btc_data:
                            funding_data['bitcoin'].binance = float(btc_data[0]['fundingRate'])
                    
                    async with session.get(eth_url) as response:
                        eth_data = await response.json()
                        if eth_data:
                            funding_data['ethereum'].binance = float(eth_data[0]['fundingRate'])
                    
                    print("✅ Binance funding rates fetched")
                except Exception as e:
                    print(f"⚠️  Binance funding error: {e}")
                
                # Bybit funding rates
                try:
                    btc_bybit_url = f"{self.base_urls['bybit']}/funding/prev-funding-rate?symbol=BTCUSDT"
                    eth_bybit_url = f"{self.base_urls['bybit']}/funding/prev-funding-rate?symbol=ETHUSDT"
                    
                    async with session.get(btc_bybit_url) as response:
                        btc_bybit_data = await response.json()
                        if btc_bybit_data.get('result'):
                            funding_data['bitcoin'].bybit = float(btc_bybit_data['result']['funding_rate'])
                    
                    async with session.get(eth_bybit_url) as response:
                        eth_bybit_data = await response.json()
                        if eth_bybit_data.get('result'):
                            funding_data['ethereum'].bybit = float(eth_bybit_data['result']['funding_rate'])
                    
                    print("✅ Bybit funding rates fetched")
                except Exception as e:
                    print(f"⚠️  Bybit funding error: {e}")
                
                # Calculate averages
                for crypto in ['bitcoin', 'ethereum']:
                    rates = [
                        funding_data[crypto].binance,
                        funding_data[crypto].bybit
                    ]
                    valid_rates = [r for r in rates if r is not None]
                    if valid_rates:
                        funding_data[crypto].average = sum(valid_rates) / len(valid_rates)
            
            print("✅ Funding rates fetched successfully")
            return funding_data
            
        except Exception as e:
            print(f"❌ Error fetching funding rates: {e}")
            return None

    async def setup_liquidation_websocket(self):
        """Set up WebSocket connection for real-time liquidation data"""
        try:
            uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
            
            async with websockets.connect(uri) as websocket:
                print("🔌 Connected to Binance liquidation stream")
                
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        if 'o' in data:
                            liquidation = LiquidationEvent(
                                symbol=data['o']['s'],
                                side=data['o']['S'],
                                quantity=float(data['o']['q']),
                                price=float(data['o']['p']),
                                value=float(data['o']['q']) * float(data['o']['p']),
                                time=datetime.fromtimestamp(data['o']['T'] / 1000).strftime('%H:%M:%S'),
                                exchange='Binance'
                            )
                            
                            # Process significant liquidations (>$100k)
                            if liquidation.value > 100000:
                                await self.process_liquidation(liquidation)
                                
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"⚠️  Error processing liquidation: {e}")
                        
        except Exception as e:
            print(f"❌ WebSocket connection error: {e}")

    async def process_liquidation(self, liquidation: LiquidationEvent):
        """Process and store significant liquidation events"""
        crypto_map = {
            'BTCUSDT': 'bitcoin',
            'ETHUSDT': 'ethereum'
        }
        
        if liquidation.symbol in crypto_map:
            crypto = crypto_map[liquidation.symbol]
            self.recent_liquidations[crypto].append(liquidation)
            
            # Keep only last 20 liquidations
            if len(self.recent_liquidations[crypto]) > 20:
                self.recent_liquidations[crypto] = self.recent_liquidations[crypto][-20:]
            
            print(f"💥 Large {liquidation.symbol} liquidation: ${liquidation.value:,.0f} - {liquidation.side}")

    def capture_643am_price(self, price_data: Dict[str, PriceData]):
        """Capture reference prices at 6:43 AM EST"""
        est_time = datetime.now().astimezone().replace(tzinfo=None)  # Local time
        
        if est_time.hour == 6 and est_time.minute == 43:
            self.reference_643am = {
                'bitcoin': price_data['bitcoin'].price,
                'ethereum': price_data['ethereum'].price,
                'timestamp': est_time.isoformat()
            }
            
            # Save to file
            self.save_reference_prices()
            print(f"📍 6:43 AM EST prices captured: BTC=${self.reference_643am['bitcoin']:,.0f}, ETH=${self.reference_643am['ethereum']:,.0f}")

    def calculate_change_from_643am(self, current_price: float, crypto: str) -> Optional[float]:
        """Calculate percentage change from 6:43 AM reference price"""
        ref_price = self.reference_643am.get(crypto)
        if ref_price is None:
            return None
        
        return ((current_price - ref_price) / ref_price) * 100

    def calculate_sentiment_score(self, price_data: Dict[str, PriceData], funding_data: Dict[str, FundingData]) -> int:
        """Calculate sentiment score based on multiple factors"""
        score = 0
        
        # Liquidation component (30%) - using recent liquidation ratios
        btc_liqs = self.recent_liquidations['bitcoin']
        eth_liqs = self.recent_liquidations['ethereum']
        
        if btc_liqs:
            long_liqs = sum(1 for liq in btc_liqs if liq.side == 'SELL')
            short_liqs = sum(1 for liq in btc_liqs if liq.side == 'BUY')
            if long_liqs + short_liqs > 0:
                liq_ratio = short_liqs / (long_liqs + short_liqs)
                score += (liq_ratio - 0.5) * 60  # -30 to +30 range
        
        # Funding rates component (25%)
        btc_funding = funding_data['bitcoin'].average or 0
        eth_funding = funding_data['ethereum'].average or 0
        avg_funding = (btc_funding + eth_funding) / 2
        
        if avg_funding > 0.02:
            score -= 25
        elif avg_funding < 0.01:
            score += 25
        else:
            score += (0.015 - avg_funding) * 1250
        
        # Volume momentum (25%)
        btc_momentum = 12.5 if price_data['bitcoin'].change_24h > 0 else -12.5
        eth_momentum = 12.5 if price_data['ethereum'].change_24h > 0 else -12.5
        score += btc_momentum + eth_momentum
        
        # Price change from 6:43 AM (20%)
        btc_643_change = self.calculate_change_from_643am(price_data['bitcoin'].price, 'bitcoin')
        eth_643_change = self.calculate_change_from_643am(price_data['ethereum'].price, 'ethereum')
        
        if btc_643_change is not None and eth_643_change is not None:
            avg_643_change = (btc_643_change + eth_643_change) / 2
            score += 20 if avg_643_change > 0 else -20
        
        return round(score)

    def save_reference_prices(self):
        """Save reference prices to file"""
        try:
            with open('reference_prices.json', 'w') as f:
                json.dump(self.reference_643am, f)
        except Exception as e:
            print(f"⚠️  Error saving reference prices: {e}")

    def load_reference_prices(self):
        """Load reference prices from file"""
        try:
            if os.path.exists('reference_prices.json'):
                with open('reference_prices.json', 'r') as f:
                    self.reference_643am = json.load(f)
                print(f"📱 Loaded reference prices: {self.reference_643am}")
        except Exception as e:
            print(f"⚠️  Error loading reference prices: {e}")

    async def fetch_all_data(self) -> Optional[MarketData]:
        """Fetch all trading data and calculate sentiment"""
        print("🚀 Fetching all trading data...")
        
        price_data = await self.get_price_data()
        funding_data = await self.get_funding_rates()
        
        if not price_data or not funding_data:
            print("❌ Failed to fetch required data")
            return None
        
        # Check if we should capture 6:43 AM prices
        self.capture_643am_price(price_data)
        
        # Calculate sentiment score
        sentiment_score = self.calculate_sentiment_score(price_data, funding_data)
        
        # Convert liquidations to dict format
        liquidations_dict = {
            'bitcoin': [asdict(liq) for liq in self.recent_liquidations['bitcoin']],
            'ethereum': [asdict(liq) for liq in self.recent_liquidations['ethereum']]
        }
        
        market_data = MarketData(
            bitcoin=price_data['bitcoin'],
            ethereum=price_data['ethereum'],
            funding_rates=funding_data,
            liquidations=liquidations_dict,
            sentiment_score=sentiment_score,
            reference_643am=self.reference_643am,
            timestamp=datetime.now(timezone.utc).isoformat()
        )
        
        print(f"📊 Sentiment Score: {sentiment_score}")
        print(f"📈 BTC: ${price_data['bitcoin'].price:,.0f} ({price_data['bitcoin'].change_24h:+.2f}%)")
        print(f"📈 ETH: ${price_data['ethereum'].price:,.0f} ({price_data['ethereum'].change_24h:+.2f}%)")
        
        return market_data

    def save_data_to_json(self, data: MarketData, filename: str = 'market_data.json'):
        """Save market data to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(asdict(data), f, indent=2, default=str)
            print(f"💾 Data saved to {filename}")
        except Exception as e:
            print(f"❌ Error saving data: {e}")

async def main():
    """Main function to run the data fetcher"""
    fetcher = CryptoDataFetcher()
    
    # Fetch initial data
    market_data = await fetcher.fetch_all_data()
    if market_data:
        fetcher.save_data_to_json(market_data)
    
    # Set up scheduled data fetching every 30 seconds
    async def scheduled_fetch():
        while True:
            try:
                data = await fetcher.fetch_all_data()
                if data:
                    fetcher.save_data_to_json(data)
                await asyncio.sleep(30)  # Wait 30 seconds
            except Exception as e:
                print(f"❌ Error in scheduled fetch: {e}")
                await asyncio.sleep(30)
    
    # Start liquidation WebSocket in background
    liquidation_task = asyncio.create_task(fetcher.setup_liquidation_websocket())
    
    # Start scheduled data fetching
    fetch_task = asyncio.create_task(scheduled_fetch())
    
    print("🎉 Crypto data fetcher running! Press Ctrl+C to stop.")
    
    try:
        await asyncio.gather(liquidation_task, fetch_task)
    except KeyboardInterrupt:
        print("\n👋 Shutting down gracefully...")

if __name__ == "__main__":
    asyncio.run(main())
