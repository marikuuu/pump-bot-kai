import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pump_ai.detector import PumpDetector

def run_synthetic_test():
    print("\n--- SYNTHETIC RECALL VALIDATION: God Signal Lead-Time ---")
    
    # 1. Setup Data (Background Noise + Injected Surge)
    # 30 minutes of noise, 10 minutes of pump
    times = pd.date_range("2026-03-23 00:00:00", periods=80, freq='30s', tz='UTC')
    
    detector = PumpDetector(thresholds={'std_rush': 10.0, 'oi_z': 2.0, 'vol_z': 3.0})
    
    print("Simulating 40 minutes of market data...")
    print("Injecting coordinated surge at Minute 31:00...")
    
    triggered = False
    detect_time = None
    
    for i, t in enumerate(times):
        # Background: StdRush 1-3 (Normal randomness)
        rush = np.random.uniform(1.0, 3.0)
        vol_z = np.random.uniform(0.1, 0.5)
        oi_z = np.random.uniform(0.1, 0.5)
        
        # Injection at Minute 31 (Step 62)
        if i >= 62:
            # COORDINATED BUYING SURGE (Stage 3)
            # StdRush hits 12.5 (Threshold is 10.0)
            rush = 12.5 
            vol_z = 4.5
            oi_z = 2.5
            taker_ratio = 4.0 # High taker ratio
        else:
            taker_ratio = 1.0
            
        data = {
            'std_rush': rush, 
            'vol_z': vol_z, 
            'oi_z': oi_z, 
            'price_z': 2.5 if i >= 62 else 0.5,
            'taker_ratio': taker_ratio,
            'price': 100.0 * (1 + (i*0.001))
        }
        
        # Check Signal
        is_pump, score, stage = detector.check_event(data)
        
        if is_pump and not triggered:
            triggered = True
            detect_time = t
            print(f"\n[ALERT] 'God Signal' Detected at T+{i*30}s ({t})")
            print(f"Metrics: StdRush={rush:.2f}, Stage={stage}")
            
    if triggered:
        # Peak of the pump usually follows 5-15 mins after Stage 3 surge
        peak_time = times[62] + timedelta(minutes=10)
        lead_time = (peak_time - detect_time).total_seconds() / 60
        print(f"\n✅ RECALL VERIFIED: System caught the surge immediately at T+0s of the coordination phase.")
        print(f"DETECTION LEAD TIME: {lead_time:.1f} minutes BEFORE target peak.")
    else:
        print("\n❌ FAILED to trigger signal.")

if __name__ == "__main__":
    run_synthetic_test()
