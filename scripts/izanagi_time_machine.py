import pandas as pd
import numpy as np
import os
from datetime import datetime

class TimeMachineBacktester:
    def __init__(self, symbol, data_dir):
        self.symbol = symbol
        self.data_dir = data_dir
        self.ohlcv = pd.read_csv(f"{data_dir}/ohlcv_1m.csv")
        self.oi_history = pd.read_csv(f"{data_dir}/oi_history.csv")
        
        # Pre-process dates
        self.ohlcv['dt'] = pd.to_datetime(self.ohlcv['ts'], unit='ms')
        self.oi_history['dt'] = pd.to_datetime(self.oi_history['timestamp'], unit='ms').dt.floor('min')
        
        # Merge for simulation
        oi_clean = self.oi_history.groupby('dt')['openInterestAmount'].last().resample('1min').ffill()
        self.data = pd.merge(self.ohlcv, oi_clean, on='dt', how='left').ffill()
        
        # Simulation Logic Parameters
        self.vol_window = 240 # 4 hours for Z-score
        self.vpvr_window = 1440 # 24 hours for Volume Profile
        
    def run_simulation(self):
        print(f"🎬 Starting Time-Machine for {self.symbol}...")
        
        results = []
        
        # Rolling stats state
        for i in range(self.vol_window, len(self.data)):
            row = self.data.iloc[i]
            window = self.data.iloc[i - self.vol_window : i]
            
            # 1. Z-Score Calculation (Accelerator)
            vol_mean = window['v'].mean()
            vol_std = window['v'].std()
            vol_z = (row['v'] - vol_mean) / (vol_std + 1e-9)
            
            # 2. Open Interest DNA (Whale Builder)
            oi_change = (row['openInterestAmount'] - window['openInterestAmount'].iloc[-1]) / window['openInterestAmount'].iloc[-1]
            price_change = abs(row['c'] - window['c'].iloc[-1]) / window['c'].iloc[-1]
            
            # "Whale Entry" = OI up > 1.5% and Price move < 0.5%
            is_whale = (oi_change > 0.015) and (price_change < 0.005)
            
            # 3. VPVR (Vacuum Zone)
            vpvr_window = self.data.iloc[max(0, i - self.vpvr_window) : i]
            price_bins = np.linspace(vpvr_window['l'].min(), vpvr_window['h'].max(), 30)
            hist, _ = np.histogram(vpvr_window['c'], bins=price_bins, weights=vpvr_window['v'])
            
            poc_idx = np.argmax(hist)
            poc_price = (price_bins[poc_idx] + price_bins[poc_idx+1]) / 2
            
            # Is price breaking into a vacuum? (Next 5% has < 20% of POC volume)
            current_price = row['c']
            vacuum_trigger = False
            if current_price > poc_price:
                # Simple check: is current price in the upper tiers of the histogram with low historical vol
                current_bin_idx = np.digitize(current_price, price_bins) - 1
                if 0 <= current_bin_idx < len(hist):
                    if hist[current_bin_idx] < hist[poc_idx] * 0.2:
                        vacuum_trigger = True

            # --- SIGNAL GENERATION ---
            signal = None
            if is_whale and vol_z > 1.5:
                signal = "🐋 WHALE ACCUMULATION"
            elif vacuum_trigger and vol_z > 3.0:
                signal = "🌌 VACUUM BREAKOUT (GHOST)"
            
            if signal:
                # Calculate potential profit if entered here vs peak
                peak_in_future = self.data.iloc[i:]['c'].max()
                profit_pot = (peak_in_future - row['c']) / row['c']
                
                # Only print if within reasonable lookback or very high profit
                time_to_peak = (self.data.iloc[self.data['c'].idxmax()]['dt'] - row['dt']).total_seconds() / 3600
                
                if time_to_peak < 24: # Focus on the last 24h
                    print(f"[{row['dt']}] 🚨 {signal} | Price: {row['c']:.4f} | Vol Z: {vol_z:.2f} | OI Δ: {oi_change:.2%} | Pot. Profit: {profit_pot:.1%}")
                
                results.append({'dt': row['dt'], 'price': row['c'], 'signal': signal, 'profit': profit_pot})
        
        # Final Peak context
        peak_idx = self.data['c'].idxmax()
        peak_price = self.data.iloc[peak_idx]['c']
        peak_dt = self.data.iloc[peak_idx]['dt']
        print(f"\n🏆 Peak Reached: {peak_price:.4f} at {peak_dt}")
        
        if results:
            first_sig = results[0]
            last_sig = results[-1]
            lead_time = (peak_dt - first_sig['dt']).total_seconds() / 60
            print(f"🥇 First Signal: {first_sig['dt']} (Lead: {lead_time/60:.1f} hours)")
            print(f"📊 Potential Profit from First Signal: {first_sig['profit']:.1%}")
            return {
                'symbol': self.symbol,
                'first_signal_dt': first_sig['dt'],
                'peak_dt': peak_dt,
                'lead_hours': lead_time / 60,
                'max_profit': first_sig['profit'],
                'signal_count': len(results)
            }
        return None

if __name__ == "__main__":
    base_dir = "data/naked_dna"
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    
    summary_list = []
    print(f"\n{'SYMBOL':<15} | {'LEAD_HR':<8} | {'PROFIT':<8} | {'SIG_CNT':<8}")
    print("-" * 50)
    
    for sym in symbols:
        try:
            tm = TimeMachineBacktester(sym, os.path.join(base_dir, sym))
            res = tm.run_simulation()
            if res:
                summary_list.append(res)
                print(f"{res['symbol']:<15} | {res['lead_hours']:<8.1f} | {res['max_profit']:<8.1%} | {res['signal_count']:<8}")
        except Exception as e:
            # print(f"Error processing {sym}: {e}")
            pass
    
    # Save summary to CSV
    if summary_list:
        pd.DataFrame(summary_list).to_csv("time_machine_summary.csv", index=False)
        print(f"\n✅ Time-Machine Summary saved to time_machine_summary.csv")
