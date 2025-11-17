"""Lock 및 성능 진단 모듈"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime

from src.database.connectors import get_connector
from src.core.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LockInfo:
    """Lock 정보"""
    session_id: str
    resource_type: str
    resource_name: str
    lock_mode: str
    lock_status: str
    wait_time: Optional[int] = None
    blocking_session: Optional[str] = None


@dataclass
class LockDiagnosis:
    """Lock 진단 결과"""
    has_locks: bool
    has_waiting: bool
    total_locks: int
    waiting_sessions: List[str]
    blocking_sessions: List[str]
    deadlock_risk: bool
    recommendations: List[str]


class LockDiagnostic:
    """Lock 및 성능 진단 클래스"""
    
    def __init__(self, db_type: str = "mssql"):
        """
        Lock 진단자 초기화
        
        Args:
            db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        """
        self.db_type = db_type.lower()
        self.db_connector = get_connector(db_type)
    
    def diagnose_locks(self) -> LockDiagnosis:
        """
        현재 Lock 상태 진단
        
        Returns:
            Lock 진단 결과
        """
        logger.info("Lock 상태 진단 시작")
        
        try:
            locks = self.db_connector.get_locks()
        except Exception as e:
            logger.error(f"Lock 정보 조회 실패: {e}")
            return LockDiagnosis(
                has_locks=False,
                has_waiting=False,
                total_locks=0,
                waiting_sessions=[],
                blocking_sessions=[],
                deadlock_risk=False,
                recommendations=["Lock 정보를 조회할 수 없습니다"]
            )
        
        # Lock 분석
        waiting_sessions = []
        blocking_sessions = []
        has_waiting = False
        
        for lock in locks:
            status = lock.get("status", "").lower() if isinstance(lock.get("status"), str) else str(lock.get("status", "")).lower()
            
            if "wait" in status or status == "waiting":
                has_waiting = True
                session_id = str(lock.get("session_id", lock.get("transaction_id", "")))
                if session_id:
                    waiting_sessions.append(session_id)
            
            # Blocking session 확인 (MSSQL의 경우)
            if "blocking" in str(lock).lower():
                blocking_id = lock.get("blocking_session_id")
                if blocking_id:
                    blocking_sessions.append(str(blocking_id))
        
        # Deadlock 위험 평가
        deadlock_risk = len(waiting_sessions) > 0 and len(blocking_sessions) > 0
        
        # 권장사항 생성
        recommendations = self._generate_recommendations(
            locks,
            waiting_sessions,
            blocking_sessions,
            deadlock_risk
        )
        
        diagnosis = LockDiagnosis(
            has_locks=len(locks) > 0,
            has_waiting=has_waiting,
            total_locks=len(locks),
            waiting_sessions=list(set(waiting_sessions)),
            blocking_sessions=list(set(blocking_sessions)),
            deadlock_risk=deadlock_risk,
            recommendations=recommendations
        )
        
        logger.info(f"Lock 상태 진단 완료: {len(locks)}개 Lock, {len(waiting_sessions)}개 대기 세션")
        return diagnosis
    
    def _generate_recommendations(
        self,
        locks: List[Dict[str, Any]],
        waiting_sessions: List[str],
        blocking_sessions: List[str],
        deadlock_risk: bool
    ) -> List[str]:
        """권장사항 생성"""
        recommendations = []
        
        if deadlock_risk:
            recommendations.append("⚠️ Deadlock 위험이 감지되었습니다. 즉시 조치가 필요합니다")
            recommendations.append("대기 중인 세션을 확인하고, 필요시 트랜잭션을 롤백하세요")
        
        if len(waiting_sessions) > 0:
            recommendations.append(f"{len(waiting_sessions)}개 세션이 Lock을 기다리고 있습니다")
            recommendations.append("ETL Job 실행 시간이 지연될 수 있습니다")
            recommendations.append("대기 중인 세션의 쿼리 실행 계획을 확인하세요")
        
        if len(blocking_sessions) > 0:
            recommendations.append(f"{len(blocking_sessions)}개 세션이 다른 세션을 차단하고 있습니다")
            recommendations.append("차단하는 세션의 트랜잭션을 확인하고, 필요시 커밋/롤백하세요")
        
        if len(locks) > 100:
            recommendations.append("Lock 수가 많습니다. 트랜잭션 격리 수준을 확인하세요")
        
        if len(recommendations) == 0:
            recommendations.append("현재 Lock 상태는 정상입니다")
        
        return recommendations
    
    def get_detailed_lock_info(self) -> Dict[str, Any]:
        """
        상세 Lock 정보 조회
        
        Returns:
            상세 Lock 정보 딕셔너리
        """
        try:
            locks = self.db_connector.get_locks()
        except Exception as e:
            logger.error(f"Lock 정보 조회 실패: {e}")
            return {}
        
        # Lock 타입별 집계
        by_type = {}
        by_mode = {}
        by_status = {}
        
        for lock in locks:
            # 타입별
            lock_type = lock.get("resource_type", "unknown")
            if lock_type not in by_type:
                by_type[lock_type] = 0
            by_type[lock_type] += 1
            
            # 모드별
            lock_mode = lock.get("mode", lock.get("lock_mode", "unknown"))
            if lock_mode not in by_mode:
                by_mode[lock_mode] = 0
            by_mode[lock_mode] += 1
            
            # 상태별
            status = lock.get("status", lock.get("lock_status", "unknown"))
            if status not in by_status:
                by_status[status] = 0
            by_status[status] += 1
        
        return {
            "total_locks": len(locks),
            "by_type": by_type,
            "by_mode": by_mode,
            "by_status": by_status,
            "locks": locks
        }
    
    def check_performance_issues(self) -> Dict[str, Any]:
        """
        성능 이슈 확인
        
        Returns:
            성능 이슈 정보 딕셔너리
        """
        diagnosis = self.diagnose_locks()
        
        performance_issues = {
            "has_issues": False,
            "issues": [],
            "severity": "low"
        }
        
        # Lock 대기 확인
        if diagnosis.has_waiting:
            performance_issues["has_issues"] = True
            performance_issues["issues"].append({
                "type": "lock_wait",
                "message": f"{len(diagnosis.waiting_sessions)}개 세션이 Lock을 기다리고 있습니다",
                "severity": "high" if len(diagnosis.waiting_sessions) > 5 else "medium"
            })
        
        # Deadlock 위험 확인
        if diagnosis.deadlock_risk:
            performance_issues["has_issues"] = True
            performance_issues["issues"].append({
                "type": "deadlock_risk",
                "message": "Deadlock 위험이 감지되었습니다",
                "severity": "critical"
            })
            performance_issues["severity"] = "critical"
        
        # Lock 수 확인
        if diagnosis.total_locks > 100:
            performance_issues["has_issues"] = True
            performance_issues["issues"].append({
                "type": "high_lock_count",
                "message": f"Lock 수가 많습니다: {diagnosis.total_locks}개",
                "severity": "medium"
            })
            if performance_issues["severity"] == "low":
                performance_issues["severity"] = "medium"
        
        return performance_issues
    
    def generate_lock_report(self) -> Dict[str, Any]:
        """
        Lock 리포트 생성
        
        Returns:
            Lock 리포트 딕셔너리
        """
        diagnosis = self.diagnose_locks()
        detailed_info = self.get_detailed_lock_info()
        performance_issues = self.check_performance_issues()
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "diagnosis": {
                "has_locks": diagnosis.has_locks,
                "has_waiting": diagnosis.has_waiting,
                "total_locks": diagnosis.total_locks,
                "waiting_sessions": diagnosis.waiting_sessions,
                "blocking_sessions": diagnosis.blocking_sessions,
                "deadlock_risk": diagnosis.deadlock_risk,
                "recommendations": diagnosis.recommendations
            },
            "detailed_info": detailed_info,
            "performance_issues": performance_issues
        }
        
        return report

