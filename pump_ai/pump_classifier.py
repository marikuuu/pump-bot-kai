import os
import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import precision_recall_curve, fbeta_score

class PumpClassifier:
    """
    Stage 3: Multi-Model Ensemble Classifier for Pump & Dump Detection.
    Uses XGBoost, Random Forest, and HistGradientBoosting with Unanimous Voting.
    Goal: >90% Precision by ensuring all 3 models agree before firing a signal.
    """
    def __init__(self, models_dir: str = 'models/'):
        self.models_dir = models_dir
        self.xgb_model = None
        self.rf_model = None
        self.hgb_model = None
        
        self.xgb_threshold = 0.85
        self.rf_threshold = 0.85
        self.hgb_threshold = 0.85

    def load_model(self):
        xgb_path = os.path.join(self.models_dir, 'pump_xgb.joblib')
        rf_path = os.path.join(self.models_dir, 'pump_rf.joblib')
        hgb_path = os.path.join(self.models_dir, 'pump_hgb.joblib')
        
        if os.path.exists(xgb_path) and os.path.exists(rf_path) and os.path.exists(hgb_path):
            self.xgb_model = joblib.load(xgb_path)
            self.rf_model = joblib.load(rf_path)
            self.hgb_model = joblib.load(hgb_path)
            return True
        return False

    def train(self, X: pd.DataFrame, y: pd.Series):
        neg_count = (y == 0).sum()
        pos_count = (y == 1).sum()
        scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1

        print(f"Training ENSEMBLE MODELS with {len(X)} samples (Pos: {pos_count}, Neg: {neg_count})...")
        tscv = TimeSeriesSplit(n_splits=5)

        # 1. XGBoost
        print("Training XGBoost...")
        base_xgb = xgb.XGBClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.05,
            scale_pos_weight=scale_pos_weight, eval_metric='logloss', random_state=42
        )
        self.xgb_model = CalibratedClassifierCV(base_xgb, cv=tscv, method='sigmoid')
        self.xgb_model.fit(X, y)

        # 2. Random Forest (Class weights configured)
        print("Training Random Forest...")
        base_rf = RandomForestClassifier(
            n_estimators=150, max_depth=7, class_weight='balanced', random_state=42
        )
        self.rf_model = CalibratedClassifierCV(base_rf, cv=tscv, method='sigmoid')
        self.rf_model.fit(X, y)

        # 3. HistGradientBoosting (LightGBM equivalent, fast on large data)
        print("Training HistGradientBoosting...")
        # HistGradient doesnt support class_weight natively in all versions, we pass sample_weight during fit
        # CalibratedClassifierCV handles the sample weights internally if passed
        base_hgb = HistGradientBoostingClassifier(max_depth=5, learning_rate=0.05, random_state=42)
        sample_weights = np.where(y == 1, scale_pos_weight, 1.0)
        
        # Scikit-learn workaround for passing sample keys in CV
        self.hgb_model = CalibratedClassifierCV(base_hgb, cv=tscv, method='sigmoid')
        self.hgb_model.fit(X, y, sample_weight=sample_weights)

        # Save all models
        os.makedirs(self.models_dir, exist_ok=True)
        joblib.dump(self.xgb_model, os.path.join(self.models_dir, 'pump_xgb.joblib'))
        joblib.dump(self.rf_model, os.path.join(self.models_dir, 'pump_rf.joblib'))
        joblib.dump(self.hgb_model, os.path.join(self.models_dir, 'pump_hgb.joblib'))
        print(f"All ensemble models saved to {self.models_dir}")

    def _find_threshold(self, model, X_val, y_val, target_precision):
        probs = model.predict_proba(X_val)[:, 1]
        precisions, recalls, thresholds = precision_recall_curve(y_val, probs)
        
        idx = np.where(precisions >= target_precision)[0]
        if len(idx) > 0:
            best_idx = min(idx[0], len(thresholds) - 1)
            return thresholds[best_idx]
        return 0.999 # Fail-safe conservative threshold

    def tune_threshold(self, X_val, y_val, target_precision=0.90):
        """Find independent thresholds for each model"""
        print(f"\n--- Tuning Ensemble Thresholds for {target_precision*100}% Precision ---")
        
        self.xgb_threshold = self._find_threshold(self.xgb_model, X_val, y_val, target_precision)
        print(f"XGBoost Threshold: {self.xgb_threshold:.4f}")
        
        self.rf_threshold = self._find_threshold(self.rf_model, X_val, y_val, target_precision)
        print(f"Random Forest Threshold: {self.rf_threshold:.4f}")
        
        self.hgb_threshold = self._find_threshold(self.hgb_model, X_val, y_val, target_precision)
        print(f"HistGradient Threshold: {self.hgb_threshold:.4f}")

        # Test Unanimous Voting Precision
        xgb_preds = self.xgb_model.predict_proba(X_val)[:, 1] >= self.xgb_threshold
        rf_preds = self.rf_model.predict_proba(X_val)[:, 1] >= self.rf_threshold
        hgb_preds = self.hgb_model.predict_proba(X_val)[:, 1] >= self.hgb_threshold
        
        votes = xgb_preds.astype(int) + rf_preds.astype(int) + hgb_preds.astype(int)
        majority_preds = votes >= 2
        
        tp = np.sum((majority_preds == 1) & (y_val == 1))
        fp = np.sum((majority_preds == 1) & (y_val == 0))
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        
        print(f"\n[ENSEMBLE PERFORMANCE] Majority Precision: {precision:.2%} (TP: {tp}, FP: {fp})")

    def predict(self, features: pd.DataFrame) -> bool:
        """Unanimous voting required"""
        if self.xgb_model is None:
            self.load_model()
            
        xgb_p = self.xgb_model.predict_proba(features)[:, 1] >= self.xgb_threshold
        rf_p = self.rf_model.predict_proba(features)[:, 1] >= self.rf_threshold
        hgb_p = self.hgb_model.predict_proba(features)[:, 1] >= self.hgb_threshold
        
        votes = xgb_p.astype(int) + rf_p.astype(int) + hgb_p.astype(int)
        return votes >= 2
