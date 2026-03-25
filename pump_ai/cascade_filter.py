import pandas as pd
import logging
from .features import FeatureEngineer

class CascadeFilter:
    """
    論文に基づく 4段階 マルチステージ・カスケードアーキテクチャ。
    計算負荷と偽陽性（False Positives）を各ステージで段階的に削減する。
    """
    def __init__(self, ml_model, db_manager):
        self.model = ml_model
        self.db = db_manager
        self.feature_engine = FeatureEngineer()

    async def stage_1_universe_filter(self, symbol: str) -> bool:
        """
        ステージ1: ルールベースフィルター（サブミリ秒）
        ターゲット: 時価総額6000万ドル未満（あるいは直近の取引活動が活発なコイン）
        """
        query = "SELECT market_cap FROM tokens WHERE symbol = $1 LIMIT 1"
        records = await self.db.fetch(query, symbol)
        if not records:
            return False # トークン情報なし
            
        cap = records[0]['market_cap']
        if cap is None:
            return True # 不明な場合は通す（保守的運用）
            
        # 例: 時価総額が 約 60,000,000 USD (60M) 未満の場合のみ通過
        if float(cap) > 60000000:
            return False
        return True

    def stage_2_statistical_anomaly(self, features_df: pd.DataFrame) -> bool:
        """
        ステージ2: 統計的異常検知（ミリ秒）
        出来高Zスコアが3σを超え、かつ価格Zスコアが2σを超えるイベント（事前蓄積パターン）
        """
        current = features_df.iloc[-1]
        vol_z = current.get('volume_zscore', 0)
        price_z = current.get('price_zscore', 0)
        
        # 簡易的に固定閾値を利用（実運用ではEWMAによる条件付き分布を用いると尚良い）
        return vol_z > 3.0 and price_z > 2.0

    def stage_3_ml_classifier(self, features_df: pd.DataFrame, threshold: float = 0.90) -> float:
        """
        ステージ3: ML分類器（XGBoost, 秒単位）
        ステージ2を通過したイベントに対して、完全な特徴量を投入して確率を算出
        """
        # モデルが予測のために必要とするカラム群
        input_data = features_df.iloc[[-1]] 
        
        # predict_proba([X])[0][1] でパンプ確率(クラス1)を取得
        try:
            proba = self.model.predict_proba(input_data)[:, 1][0]
            if proba >= threshold:
                return proba
        except Exception as e:
            logging.error(f"Stage 3 ML inference error: {e}")
        return 0.0

    async def stage_4_social_whale_confirmation(self, symbol: str, time_window_minutes: int = 60) -> bool:
        """
        ステージ4: ソーシャルシグナルおよびオンチェーン（クジラ）確認
        Alchemy/Arkham等のトラッカーによって、直近60分以内にスマートマネーの
        継続的な買い集め（dex_swaps）が存在したかをクエリする。
        """
        query = """
            SELECT COUNT(*) as whale_buys
            FROM dex_swaps
            WHERE (token_out_address = $1 OR symbol = $1)
              AND is_smart_money = TRUE
              AND time >= NOW() - INTERVAL '$2 minutes'
        """
        # 注意: SQl injection やパラメータ扱いの簡略化のため、実運用では整形が必要
        records = await self.db.fetch(query, symbol, time_window_minutes)
        if records and records[0]['whale_buys'] > 0:
            return True
        return False

    async def run_pipeline(self, symbol: str, ohlcv_df: pd.DataFrame, oi_df: pd.DataFrame = None):
        """完全なカスケードパイプラインを実行"""
        # 1. ユニバースフィルタ
        if not await self.stage_1_universe_filter(symbol):
            return False, "Failed Stage 1: Market Cap or Activity"
            
        # 特徴量抽出
        features_df = self.feature_engine.generate_features(ohlcv_df, oi_df)
        if features_df.empty:
            return False, "Not enough data for features"

        # 2. 統計的異常
        if not self.stage_2_statistical_anomaly(features_df):
            return False, "Failed Stage 2: Not a statistical anomaly"

        # 3. XGBoost 分類
        proba = self.stage_3_ml_classifier(features_df)
        if proba < 0.90:
            return False, f"Failed Stage 3: ML Probability too low ({proba:.2f})"

        # 4. クジラ確認
        if not await self.stage_4_social_whale_confirmation(symbol):
            # MLが突破しただけでも有力だが、90%以上の精度を確約するにはクジラの裏付けが必要
            return True, f"Passed Stage 3 ({proba:.2f}) but no Whale confirmation (Stage 4)"

        return True, f"Passed all stages! Extreme Confidence Pump Target ({proba:.2f})"
