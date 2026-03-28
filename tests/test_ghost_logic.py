import asyncio
import logging
import sys
import os

# Ensure the project root is in path
sys.path.append(os.getcwd())

from pump_ai.detector import PumpDetector

async def test_ghost_logic():
    detector = PumpDetector()
    logging.info("Testing Protocol GHOST Triggers (Whale Accumulation + Vacuum)...")

    # Case 1: Whale Accumulation + Vacuum (GHOST TRIGGER)
    features_accum = {
        'symbol': 'GHOST_ACCUM_1/USDT',
        'exchange': 'binance',
        'price': 1.0,
        'vol_z': 3.5,
        'pc_z': 0.1,
        'oi_z': 4.5,
        'oi_change': 0.05,
        'pc_change': 0.001,
        'vacuum_score': 0.95,
        'pre_accum_z': 1.2,
        'std_rush': 5.0,
        'buy_ratio': 0.6,
        'market_cap': 10_000_000
    }
    
    detector.check_event(features_accum)
    detector.check_event(features_accum)
    is_p1, score1, stage1 = detector.check_event(features_accum)
    print(f"Accum 1 (Binance): is_pump={is_p1}, stage={stage1}")
    
    features_accum['exchange'] = 'bybit'
    is_p2, score2, stage2 = detector.check_event(features_accum)
    print(f"Accum 2 (Bybit): is_pump={is_p2}, stage={stage2}")
    assert is_p2 == True

    # Case 2: Standard Pump Breakout
    features_pump = {
        'symbol': 'STD_PUMP_1/USDT',
        'exchange': 'binance',
        'price': 1.1,
        'vol_z': 5.0,
        'pc_z': 2.0,
        'oi_z': 1.0,
        'oi_change': 0.01,
        'pc_change': 0.02,
        'vacuum_score': 0.2,
        'pre_accum_z': 0.5,
        'std_rush': 15.0,
        'buy_ratio': 0.7,
        'market_cap': 50_000_000
    }
    
    res1 = detector.check_event(features_pump)
    print(f"Standard 1 (Binance): is_pump={res1[0]}, stage={res1[2]}")
    
    features_pump['exchange'] = 'bybit'
    res2 = detector.check_event(features_pump)
    print(f"Standard 2 (Bybit): is_pump={res2[0]}, stage={res2[2]}")
    
    if not res2[0]:
        print(f"DEBUG: Stage 1 Filter: MCAP={features_pump['market_cap']} MAX={detector.MAX_MARKET_CAP}")
        print(f"DEBUG: Stage 2 Filter: VOLZ={features_pump['vol_z']} PCZ={features_pump['pc_z']}")
        print(f"DEBUG: Stage 3 Filter: RUSH={features_pump['std_rush']}")
        
    assert res2[0] == True

    print("✅ All Protocol GHOST Mock Tests Passed!")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_ghost_logic())
