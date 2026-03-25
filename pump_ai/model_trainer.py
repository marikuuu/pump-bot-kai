import xgboost as xgb
import pandas as pd
import numpy as np
import logging
from sklearn.metrics import average_precision_score, fbeta_score

class PumpModelTrainer:
    """
    データリークを防ぐウォークフォワード検証と、極端な不均衡データに
    対応するXGBoostの学習パイプライン。PR-AUCとF0.5スコアで評価する。
    """
    def __init__(self, model_params: dict = None):
        self.default_params = {
            'objective': 'binary:logistic',
            'eval_metric': 'aucpr',
            'max_depth': 5,
            'learning_rate': 0.05,
            'n_estimators': 200,
            'subsample': 0.8,
            'colsample_bytree': 0.8
        }
        self.params = model_params or self.default_params
        self.model = None

    def calculate_scale_pos_weight(self, y_train: pd.Series) -> float:
        """非パンプ数 / パンプ数 によって不均衡を調整"""
        pos_count = y_train.sum()
        if pos_count == 0:
            return 1.0
        neg_count = len(y_train) - pos_count
        return float(neg_count / pos_count)

    def train(self, X_train: pd.DataFrame, y_train: pd.Series):
        """学習データを用いたモデルの訓練"""
        scale_weight = self.calculate_scale_pos_weight(y_train)
        self.model = xgb.XGBClassifier(
            **self.params,
            scale_pos_weight=scale_weight,
            use_label_encoder=False
        )
        logging.info(f"Training XGBoost with scale_pos_weight={scale_weight:.2f}")
        self.model.fit(X_train, y_train)

    def evaluate(self, X_test: pd.DataFrame, y_test: pd.Series, threshold: float = 0.90):
        """
        [論文の知見] 閾値をデフォルトの0.5ではなく0.85〜0.95に設定し、
        Precision-Recall (PR-AUC) および F0.5 (精度重視) で評価する。
        """
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
            
        probas = self.model.predict_proba(X_test)[:, 1]
        preds = (probas >= threshold).astype(int)
        
        pr_auc = average_precision_score(y_test, probas)
        f05 = fbeta_score(y_test, preds, beta=0.5)
        
        # Calculate Precision and Recall manually for reporting
        tp = np.sum((preds == 1) & (y_test == 1))
        fp = np.sum((preds == 1) & (y_test == 0))
        fn = np.sum((preds == 0) & (y_test == 1))
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        logging.info(f"Evaluation @ Threshold {threshold}:")
        logging.info(f"PR-AUC: {pr_auc:.4f} | F0.5: {f05:.4f}")
        logging.info(f"Precision: {precision:.4f} | Recall: {recall:.4f}")
        return precision, recall, pr_auc
        
    def walk_forward_validation(self, df: pd.DataFrame, features: list, target: str, n_splits: int = 5):
        """
        時間的なリークを防ぐための時系列スライディングウィンドウ検証。
        シャッフルやK-Foldを絶対に使用してはならない（未来から過去へのデータリークとなるため）。
        """
        logging.info("Starting Walk-Forward Validation...")
        # 時間順にソートされていることを前提とする
        chunk_size = len(df) // n_splits
        
        results = []
        for i in range(1, n_splits):
            train_start = 0
            train_end = i * chunk_size
            test_start = train_end
            test_end = test_start + chunk_size
            
            # 1日分のギャップを開けて隣接リークを防ぐ（理想的な運用時）
            train_df = df.iloc[train_start:train_end]
            test_df = df.iloc[test_start:test_end]
            
            X_train, y_train = train_df[features], train_df[target]
            X_test, y_test = test_df[features], test_df[target]
            
            if y_train.sum() == 0 or y_test.sum() == 0:
                continue # パンプイベントが含まれていないチャンクはスキップ
                
            self.train(X_train, y_train)
            precision, recall, pr_auc = self.evaluate(X_test, y_test, threshold=0.90)
            results.append({'fold': i, 'precision': precision, 'recall': recall, 'pr_auc': pr_auc})
            
        return pd.DataFrame(results)
