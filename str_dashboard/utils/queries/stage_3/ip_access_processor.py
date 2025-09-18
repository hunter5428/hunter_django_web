# str_dashboard/utils/queries/stage_3/ip_access_processor.py
"""
IP 접속 이력 처리 모듈
"""

import logging
import pandas as pd
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class IPAccessProcessor:
    """
    Stage 3: IP 접속 이력 데이터 처리 클래스
    """
    
    def __init__(self):
        self.ip_access_df = None
        self.statistics = {}
        
    def process(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        IPAccessExecutor의 실행 결과를 처리
        
        Args:
            execution_result: IPAccessExecutor.execute()의 반환값
            
        Returns:
            처리된 결과
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframe(execution_result)
            
            # 데이터 분석
            analysis = self._analyze_ip_patterns()
            
            # Export용 데이터 준비
            export_data = self._prepare_export_data(execution_result, analysis)
            
            return {
                'success': True,
                'dataframes': {
                    'ip_access': self.ip_access_df,
                    'summary': self._create_summary_df(analysis)
                },
                'analysis': analysis,
                'export_data': export_data
            }
            
        except Exception as e:
            logger.exception(f"[Stage 3 Processor] Error: {e}")
            return {
                'success': False,
                'message': f"IP 접속 데이터 처리 실패: {str(e)}"
            }
    
    def _create_dataframe(self, execution_result: Dict[str, Any]):
        """DataFrame 생성"""
        ip_data = execution_result.get('ip_access_data', {})
        
        if ip_data.get('columns') and ip_data.get('rows'):
            self.ip_access_df = pd.DataFrame(
                ip_data['rows'],
                columns=ip_data['columns']
            )
            
            # 접속일시를 datetime으로 변환
            if '접속일시' in self.ip_access_df.columns:
                self.ip_access_df['접속일시'] = pd.to_datetime(
                    self.ip_access_df['접속일시'], 
                    format='%Y-%m-%d %H:%M:%S',
                    errors='coerce'
                )
            
            logger.info(f"[Stage 3 Processor] IP access DF: {self.ip_access_df.shape}")
        else:
            self.ip_access_df = pd.DataFrame()
        
        self.statistics = execution_result.get('statistics', {})
    
    def _analyze_ip_patterns(self) -> Dict[str, Any]:
        """IP 접속 패턴 분석"""
        if self.ip_access_df is None or self.ip_access_df.empty:
            return {'has_data': False}
        
        analysis = {
            'has_data': True,
            'total_access': len(self.ip_access_df),
            'time_patterns': {},
            'location_patterns': {},
            'risk_indicators': {}
        }
        
        # 시간대별 분석
        if '접속일시' in self.ip_access_df.columns:
            df = self.ip_access_df.copy()
            df['hour'] = df['접속일시'].dt.hour
            df['weekday'] = df['접속일시'].dt.weekday
            
            # 시간대별 접속
            hour_dist = df['hour'].value_counts().to_dict()
            analysis['time_patterns']['by_hour'] = dict(sorted(hour_dist.items()))
            
            # 요일별 접속 (0=월요일, 6=일요일)
            weekday_dist = df['weekday'].value_counts().to_dict()
            analysis['time_patterns']['by_weekday'] = dict(sorted(weekday_dist.items()))
            
            # 새벽 접속 비율 (00:00 ~ 06:00)
            dawn_access = df[df['hour'].between(0, 5)].shape[0]
            analysis['time_patterns']['dawn_access_rate'] = round(dawn_access / len(df), 3)
        
        # 위치 패턴 분석
        if '국가한글명' in self.ip_access_df.columns:
            country_counts = self.ip_access_df['국가한글명'].value_counts()
            analysis['location_patterns']['top_countries'] = country_counts.head(5).to_dict()
            
            # 해외 접속 비율
            foreign_access = self.ip_access_df[
                self.ip_access_df['국가한글명'] != '대한민국'
            ].shape[0]
            analysis['location_patterns']['foreign_rate'] = round(
                foreign_access / len(self.ip_access_df), 3
            )
        
        # IP 변경 빈도
        if 'IP주소' in self.ip_access_df.columns:
            unique_ips = self.ip_access_df['IP주소'].nunique()
            analysis['location_patterns']['unique_ips'] = unique_ips
            
            # IP당 평균 접속 횟수
            analysis['location_patterns']['avg_access_per_ip'] = round(
                len(self.ip_access_df) / max(unique_ips, 1), 2
            )
        
        # 위험 지표
        analysis['risk_indicators'] = self._calculate_risk_indicators(analysis)
        
        return analysis
    
    def _calculate_risk_indicators(self, analysis: Dict) -> Dict[str, Any]:
        """위험 지표 계산"""
        indicators = {
            'foreign_access': 'LOW',
            'dawn_access': 'LOW',
            'ip_diversity': 'LOW',
            'overall_risk': 'LOW'
        }
        
        risk_score = 0
        
        # 해외 접속 평가
        foreign_rate = analysis.get('location_patterns', {}).get('foreign_rate', 0)
        if foreign_rate > 0.5:
            indicators['foreign_access'] = 'HIGH'
            risk_score += 3
        elif foreign_rate > 0.2:
            indicators['foreign_access'] = 'MEDIUM'
            risk_score += 2
        else:
            risk_score += 1
        
        # 새벽 접속 평가
        dawn_rate = analysis.get('time_patterns', {}).get('dawn_access_rate', 0)
        if dawn_rate > 0.3:
            indicators['dawn_access'] = 'HIGH'
            risk_score += 3
        elif dawn_rate > 0.1:
            indicators['dawn_access'] = 'MEDIUM'
            risk_score += 2
        else:
            risk_score += 1
        
        # IP 다양성 평가
        unique_ips = analysis.get('location_patterns', {}).get('unique_ips', 0)
        total_access = analysis.get('total_access', 1)
        ip_diversity = unique_ips / max(total_access, 1)
        
        if ip_diversity > 0.5:
            indicators['ip_diversity'] = 'HIGH'
            risk_score += 3
        elif ip_diversity > 0.2:
            indicators['ip_diversity'] = 'MEDIUM'
            risk_score += 2
        else:
            risk_score += 1
        
        # 전체 위험도
        avg_risk = risk_score / 3
        if avg_risk >= 2.5:
            indicators['overall_risk'] = 'HIGH'
        elif avg_risk >= 1.5:
            indicators['overall_risk'] = 'MEDIUM'
        
        indicators['risk_score'] = round(avg_risk * 33.33)  # 0-100 scale
        
        return indicators
    
    def _create_summary_df(self, analysis: Dict[str, Any]) -> pd.DataFrame:
        """분석 요약 DataFrame 생성"""
        if not analysis.get('has_data'):
            return pd.DataFrame()
        
        summary_data = []
        
        # 기본 통계
        summary_data.append({
            'Category': '총 접속 횟수',
            'Value': analysis.get('total_access', 0),
            'Description': '조회 기간 내 총 접속 횟수'
        })
        
        # 위치 패턴
        location = analysis.get('location_patterns', {})
        summary_data.append({
            'Category': '고유 IP 수',
            'Value': location.get('unique_ips', 0),
            'Description': '사용된 고유한 IP 주소 개수'
        })
        
        summary_data.append({
            'Category': '해외 접속 비율',
            'Value': f"{location.get('foreign_rate', 0) * 100:.1f}%",
            'Description': '전체 접속 중 해외 접속 비율'
        })
        
        # 시간 패턴
        time_patterns = analysis.get('time_patterns', {})
        summary_data.append({
            'Category': '새벽 접속 비율',
            'Value': f"{time_patterns.get('dawn_access_rate', 0) * 100:.1f}%",
            'Description': '00시-06시 사이 접속 비율'
        })
        
        # 위험 지표
        risk = analysis.get('risk_indicators', {})
        summary_data.append({
            'Category': '위험도 점수',
            'Value': f"{risk.get('risk_score', 0)}점",
            'Description': '종합 위험도 점수 (0-100)'
        })
        
        summary_data.append({
            'Category': '위험 수준',
            'Value': risk.get('overall_risk', 'LOW'),
            'Description': 'LOW/MEDIUM/HIGH'
        })
        
        return pd.DataFrame(summary_data)
    
    def _prepare_export_data(self, execution_result: Dict[str, Any], 
                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Export용 데이터 준비"""
        export_data = {
            'stage': 'stage_3',
            'mem_id': execution_result.get('summary', {}).get('mem_id'),
            'period': execution_result.get('summary', {}).get('period'),
            'statistics': self.statistics,
            'analysis': analysis,
            'dataframes': {}
        }
        
        if self.ip_access_df is not None and not self.ip_access_df.empty:
            # datetime을 문자열로 변환
            df_export = self.ip_access_df.copy()
            if '접속일시' in df_export.columns:
                df_export['접속일시'] = df_export['접속일시'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            export_data['dataframes']['ip_access'] = {
                'columns': df_export.columns.tolist(),
                'rows': df_export.values.tolist()
            }
        
        return export_data