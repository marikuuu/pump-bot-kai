import logging
import pandas as pd
import numpy as np
from typing import Dict, Optional

class PumpDetector:
    """
    Orchestrates the 4-Stage Cascade Architecture for Pump & Dump Detection.
    Accuracy Objective: 90%+ Precision (v5.0 Tick-Level DNA).
    """
    def __init__(self, classifier_path='pump_ai/pump_model_v51_diverse.pkl', thresholds: Dict[str, float] = None):
        import pickle
        import os
        self.model = None
        if os.path.exists(classifier_path):
            try:
                with open(classifier_path, 'rb') as f:
                    self.model = pickle.load(f)
                logging.info(f"✅ Loaded ML Model v5.1 (Diverse): {classifier_path}")
            except Exception as e:
                logging.error(f"Failed to load ML model: {e}")

        # BTC Filter State
        self.btc_change_15m = 0.0
        self.btc_change_1h = 0.0
        self.BTC_CRASH_THRESHOLD = -0.01 # -1.0% in 15min implies danger

        # Thresholds from multiverse validation
        self.MAX_MARKET_CAP     = thresholds.get('market_cap', 1_000_000_000) if thresholds else 1_000_000_000
        self.VOL_Z_THRESHOLD    = thresholds.get('vol_z',    2.0) if thresholds else 2.0
        self.PC_Z_THRESHOLD     = thresholds.get('pc_z',     1.5) if thresholds else 1.5
        self.OI_Z_THRESHOLD     = thresholds.get('oi_z',     2.0) if thresholds else 2.0
        self.ML_THRESHOLD       = 0.70  # Universal diversity threshold

        # Signal Management
        self.signal_history: Dict[str, float] = {} # {symbol: last_alert_time}
        self.cross_ex_window: Dict[str, Dict] = {} # {symbol: {'exchanges': set(), 'time': start_time}}
        self.ALERT_COOLDOWN = 43200 # 12 hours
        self.CROSS_EX_WINDOW = 300 # 5 minutes

    def stage_1_universe_filter(self, symbol: str, market_cap: float, exchange_count: int) -> bool:
        if market_cap > self.MAX_MARKET_CAP: 
            return False
        return True

    def stage_2_statistical_anomaly(self, vol_z: float, pc_z: float, oi_z: float = 0.0) -> bool:
        return (vol_z > self.VOL_Z_THRESHOLD and pc_z > self.PC_Z_THRESHOLD) or \
               (oi_z > self.OI_Z_THRESHOLD and pc_z > 1.0)

    def stage_3_ml_classifier(self, features: Dict[str, float]) -> (bool, int):
        if self.model is None:
            return features.get('std_rush', 0) > 8.0, 3
        X = np.array([[
            features.get('vol_z', 0), features.get('pc_z', 0), features.get('pre_accum_z', 0),
            features.get('std_rush', 0), features.get('avg_trade_size', 0),
            features.get('max_trade_size', 0), features.get('median_trade_size', 0),
            features.get('buy_ratio', 0.5), features.get('acceleration', 1.0),
            features.get('price_impact', 0.0)
        ]])
        try:
            proba = self.model.predict_proba(X)[0, 1]
            if proba >= self.ML_THRESHOLD:
                logging.warning(f"🎯 ML v5.1 HIT: Confidence {proba:.2%}")
                return True, 3
        except Exception as e:
            logging.error(f"ML Prediction Error: {e}")
        return False, 0

    def set_btc_status(self, change_15m: float, change_1h: float):
        self.btc_change_15m = change_15m
        self.btc_change_1h = change_1h

    def is_market_safe(self) -> bool:
        if self.btc_change_15m <= self.BTC_CRASH_THRESHOLD:
            logging.warning(f"⚠️ BTC CIRCUIT BREAKER: BTC is down {self.btc_change_15m:.2%} (Limit: {self.BTC_CRASH_THRESHOLD:.2%})")
            return False
        return True

    def check_event(self, market_data: Dict[str, float]) -> (bool, float, int):
        import time
        symbol = market_data.get('symbol', 'Unknown')
        exchange = market_data.get('exchange', 'UNKNOWN')
        now = time.time()
        
        # New: BTC Circuit Breaker (Stage 0)
        if not self.is_market_safe():
            return False, 0.0, 0

        # Stage 1: Universe Filter
        if not self.stage_1_universe_filter(symbol, market_data.get('market_cap', 0), market_data.get('exchanges', 1)):
            return False, 0.0, 0
        
        # Stage 2: Statistical Anomaly
        vol_z = market_data.get('vol_z', 0)
        pc_z  = market_data.get('pc_z', 0)
        oi_z  = market_data.get('oi_z', 0)
        
        if not self.stage_2_statistical_anomaly(vol_z, pc_z, oi_z):
            return False, 0.0, 1
            
        # Stage 3: Micro-structure + ML
        is_pump, stage = self.stage_3_ml_classifier(market_data)
        if not is_pump:
            return False, 0.0, 2

        # === STAGE 4: DUAL-SENTINEL (CROSS-EX CONFIRMATION) ===
        last_alert = self.signal_history.get(symbol, 0)
        is_new_cycle = (now - last_alert) > self.ALERT_COOLDOWN
        
        # Update Cross-Ex Window (Reduce to 60s for ultra-precision)
        CONFIRM_WINDOW = 60 
        if symbol not in self.cross_ex_window or (now - self.cross_ex_window[symbol]['time']) > CONFIRM_WINDOW:
            # First hit on this symbol in this window
            self.cross_ex_window[symbol] = {'exchanges': {exchange}, 'time': now, 'first_alert': False}
            logging.info(f"⏳ [PENDING] {symbol} detected on {exchange}. Waiting for confirmation...")
            return False, 0.0, 3 # Stage 3 passed, but Stage 4 pending
        
        # Subsequent hit in window
        already_detected = exchange in self.cross_ex_window[symbol]['exchanges']
        self.cross_ex_window[symbol]['exchanges'].add(exchange)
        
        # Confirmation Logic (Only alert if 2+ exchanges and NOT already alerted this cycle)
        cross_ex_hit = len(self.cross_ex_window[symbol]['exchanges']) >= 2
        
        if cross_ex_hit and is_new_cycle and not self.cross_ex_window[symbol]['first_alert']:
            self.cross_ex_window[symbol]['first_alert'] = True # Ensure only one alert per sync burst
            ex_list = ",".join(self.cross_ex_window[symbol]['exchanges'])
            logging.warning(f"💎 [CONFIRMED] {symbol} CROSS-EX SYNC on {ex_list}!")
            
            self.signal_history[symbol] = now
            
            from pump_ai.notifier import DiscordNotifier
            try:
                notifier = DiscordNotifier()
                import asyncio
                asyncio.create_task(notifier.send_pump_alert(
                    symbol=symbol,
                    lead_time=f"MULTI-EX SYNC ({ex_list})",
                    move="🚀 [PRECISION 80%+] Global Accumulation Confirmed",
                    price=market_data.get('price', 0),
                    vol_z=vol_z,
                    pc_z=pc_z,
                    oi_z=oi_z,
                    rush=market_data.get('std_rush', 0),
                ))
            except Exception as e:
                logging.error(f"Notification error: {e}")
            
            return True, 0.99, 4 # Stage 4 Passed
            
        return False, 0.0, 3
