import pandas as pd
import numpy as np

class FeatureEngineer:
    """
    論文に基づくパンプ検知用のML特徴量計算エンジン。
    リアルタイムでもバックテストでも完全に同じロジックを再利用できるよう、
    状態を持たない純粋関数として実装。データリークの防止に主眼を置く。
    """
    
    @staticmethod
    def calculate_rolling_zscore(series: pd.Series, window: int) -> pd.Series:
        """過去N期間の移動平均と標準偏差を用いたローリングZスコアの計算。"""
        rolling = series.rolling(window=window, min_periods=window//2)
        return (series - rolling.mean()) / rolling.std()

    @staticmethod
    def calculate_volume_spike_ratio(volume_series: pd.Series, window: int = 20) -> pd.Series:
        """
        20日(あるいは20期間)EWMAに対する出来高スパイク比率
        ベースラインの300%〜500%を検知する（論文実証済みの重要特徴量）。
        """
        ewma_volume = volume_series.ewm(span=window, adjust=False).mean()
        # ゼロ除算防止
        return volume_series / ewma_volume.replace(0, np.nan)

    @staticmethod
    def calculate_rush_orders_std(trades_df: pd.DataFrame, window: int) -> pd.Series:
        """
        [論文で最も重要視された特徴量: Gini重要度1位]
        ラッシュオーダー（任意の価格で即座に約定した成行買い注文）の標準偏差。
        """
        if 'is_buyer_maker' not in trades_df.columns:
            return pd.Series(index=trades_df.index, dtype=float)
            
        # Taker買い（is_buyer_maker=False）のボリューム連続性を計算
        taker_buys = trades_df[~trades_df['is_buyer_maker']]['volume']
        return taker_buys.rolling(window=window, min_periods=1).std()

    def generate_features(self, ohlcv_df: pd.DataFrame, oi_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        DataFrameから全プロセス特徴量を抽出し、XGBoostに直接入力できる形にする。
        """
        df = ohlcv_df.copy()
        
        # 1. 必須特徴量 (Tier 1) - 時系列的な変化率とZスコア
        df['price_change'] = df['close'].pct_change()
        df['price_zscore'] = self.calculate_rolling_zscore(df['price_change'], window=24) 
        df['volume_zscore'] = self.calculate_rolling_zscore(df['volume'], window=24)
        df['volume_spike_ratio'] = self.calculate_volume_spike_ratio(df['volume'], window=20)
        
        # 2. オーダーブック不均衡 / OI の統合 (Tier 2相当)
        if oi_df is not None and not oi_df.empty:
            df = df.join(oi_df[['oi_asset']].rename(columns={'oi_asset': 'open_interest'}), how='left')
            df['oi_change'] = df['open_interest'].pct_change()
            df['oi_zscore'] = self.calculate_rolling_zscore(df['oi_change'], window=24)
            # OIとVolumeの比率（建玉の積み上がりに対する現物/先物取引量の強さ）
            df['oi_volume_ratio'] = df['open_interest'] / df['volume'].replace(0, np.nan)
        
        # MLモデルに入力するため前方補完と欠損行の削除
        return df.fillna(method='ffill').dropna()
