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

        # Thresholds from multiverse validation (v5.3 Golden Ratio)
        self.MAX_MARKET_CAP     = 200_000_000   # Lowered to target 500-1000 rank gems
        self.VOL_Z_THRESHOLD    = 3.0           # Strict volume surge requirement
        self.PC_Z_THRESHOLD     = 1.5           # Strict price pump requirement
        self.OI_Z_THRESHOLD     = 2.0
        self.ML_THRESHOLD       = 0.85          # Higher precision requirement

        # Protocol GHOST (v6.0) - Whale Accumulation & Vacuum Zone
        self.WHALE_OI_THRESHOLD = 0.015    # 1.5% OI growth in 1 min
        self.WHALE_PC_MAX      = 0.005    # Price must be stable (< 0.5%)
        self.VACUUM_SCORE_MIN  = 0.8      # 80% of upper range is "empty"
        
        # Signal Management
        self.signal_history: Dict[str, float] = {} # {symbol: last_alert_time}
        self.cross_ex_window: Dict[str, Dict] = {} # {symbol: {'exchanges': set(), 'time': start_time}}
        self.whale_stack: Dict[str, int] = {}      # {symbol: count} - Track "Whale footprint" stack
        self.ALERT_COOLDOWN = 43200 # 12 hours
        self.CROSS_EX_WINDOW = 300 # 5 minutes

    def is_market_safe(self) -> bool:
        """ Returns true if BTC is NOT crashing. """
        return self.btc_change_15m > self.BTC_CRASH_THRESHOLD

    def stage_1_universe_filter(self, symbol: str, market_cap: float, exchange_count: int) -> bool:
        if market_cap > self.MAX_MARKET_CAP: 
            return False
        return True

    def stage_2_statistical_anomaly(self, vol_z: float, pc_z: float, oi_z: float = 0.0) -> bool:
        # Standard surge detection
        return (vol_z > self.VOL_Z_THRESHOLD and pc_z > self.PC_Z_THRESHOLD) or \
               (oi_z > self.OI_Z_THRESHOLD and pc_z > 1.0)

    def stage_2_5_whale_accumulator(self, oi_change: float, price_change: float) -> bool:
        """ Detects if whales are building positions without moving price. """
        return (oi_change > self.WHALE_OI_THRESHOLD) and (abs(price_change) < self.WHALE_PC_MAX)

    def stage_2_6_vacuum_check(self, vacuum_score: float) -> bool:
        """ Confirm if price is in a low-resistance 'Vacuum Zone'. """
        return vacuum_score >= self.VACUUM_SCORE_MIN

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
                return True, 3
        except Exception as e:
            logging.error(f"ML Prediction Error: {e}")
        return False, 0

    def check_event(self, market_data: Dict[str, float]) -> (bool, float, int):
        import time
        symbol = market_data.get('symbol', 'Unknown')
        exchange = market_data.get('exchange', 'UNKNOWN')
        now = time.time()
        
        # Stage 0: Market Safety
        if not self.is_market_safe():
            return False, 0.0, 0

        # Stage 1: Universe Filter
        if not self.stage_1_universe_filter(symbol, market_data.get('market_cap', 0), market_data.get('exchanges', 1)):
            return False, 0.0, 0
        
        # --- [GHOST] Stage 2.5: Whale Build Filter ---
        oi_change = market_data.get('oi_change', 0)
        pc_change = market_data.get('pc_change', 0)
        is_whale_build = self.stage_2_5_whale_accumulator(oi_change, pc_change)
        
        if is_whale_build:
            self.whale_stack[symbol] = self.whale_stack.get(symbol, 0) + 1
            logging.info(f"🐋 [WHALE] {symbol} Accumulation detected! Stack: {self.whale_stack[symbol]}")
            # Special logic: if whale stack is high, we lower the breakout thresholds
        
        # Stage 2: Statistical Anomaly
        vol_z = market_data.get('vol_z', 0)
        pc_z  = market_data.get('pc_z', 0)
        oi_z  = market_data.get('oi_z', 0)
        
        passed_s2 = self.stage_2_statistical_anomaly(vol_z, pc_z, oi_z)
        if not passed_s2 and not (is_whale_build and vol_z > 2.0):
            return False, 0.0, 1

        # --- [GHOST] Stage 2.6: Vacuum Filter ---
        v_score = market_data.get('vacuum_score', 0)
        is_vacuum = self.stage_2_6_vacuum_check(v_score)
        
        # Stage 3: Micro-structure + ML
        is_pump, stage = self.stage_3_ml_classifier(market_data)
        
        # GHOST Override: if whale building + vacuum, we pass Stage 3 even if ML is shy
        ghost_hit = self.whale_stack.get(symbol, 0) >= 3 and is_vacuum and vol_z > 3.0
        if not is_pump and not ghost_hit:
            return False, 0.0, 2

        # Stage 4: Dual-Sentinel (Cross-Ex Sync)
        last_alert = self.signal_history.get(symbol, 0)
        is_new_cycle = (now - last_alert) > self.ALERT_COOLDOWN
        
        CONFIRM_WINDOW = 60 
        if symbol not in self.cross_ex_window or (now - self.cross_ex_window[symbol]['time']) > CONFIRM_WINDOW:
            self.cross_ex_window[symbol] = {'exchanges': {exchange}, 'time': now, 'first_alert': False}
            return False, 0.0, 3 # Waiting for 2nd exchange
        
        self.cross_ex_window[symbol]['exchanges'].add(exchange)
        cross_ex_hit = len(self.cross_ex_window[symbol]['exchanges']) >= 2
        
        if cross_ex_hit and is_new_cycle and not self.cross_ex_window[symbol]['first_alert']:
            self.cross_ex_window[symbol]['first_alert'] = True
            ex_list = ",".join(self.cross_ex_window[symbol]['exchanges'])
            
            # 🔥 Protocol GHOST Signal 🔥
            lead_type = "GHOST TRIGGER (Whale+Vacuum)" if ghost_hit else "STANDARD PUMP"
            logging.warning(f"👻 [PROTOCOL GHOST] {symbol} {lead_type} on {ex_list}!")
            
            self.signal_history[symbol] = now
            
            from pump_ai.notifier import DiscordNotifier
            try:
                notifier = DiscordNotifier()
                import asyncio
                asyncio.create_task(notifier.send_pump_alert(
                    symbol=symbol,
                    lead_time=lead_type,
                    move="🚀 1.5x - 2.0x (Potential)",
                    price=market_data.get('price', 0),
                    vol_z=vol_z, pc_z=pc_z, oi_z=oi_z, 
                    rush=market_data.get('std_rush', 0),
                    whale_stack=self.whale_stack.get(symbol, 0),
                    vacuum_score=v_score,
                    is_ghost=ghost_hit
                ))
            except Exception as e: logging.error(f"Notifier error: {e}")
            
            return True, 0.99, 4
            
        return False, 0.0, 3
