"""DataStage 연동 모듈"""

from src.datastage.api_client import DataStageAPIClient
from src.datastage.designer_client import DataStageDesignerClient
from src.datastage.ssh_client import DataStageSSHClient
from src.datastage.local_client import DataStageLocalClient
from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.datastage.job_modifier import JobModifier

# Java SDK는 선택적으로 import (Segmentation fault 문제로 인해)
try:
    from src.datastage.java_sdk_client import DataStageJavaSDKClient
    JAVA_SDK_AVAILABLE = True
except (ImportError, Exception):
    DataStageJavaSDKClient = None
    JAVA_SDK_AVAILABLE = False

# 편리한 인터페이스 제공
def get_datastage_client(use_java_sdk: bool = None):
    """
    DataStage 클라이언트 인스턴스 생성
    
    Args:
        use_java_sdk: Java SDK 사용 여부 (None이면 config에서 자동 결정)
    
    Returns:
        DataStageAPIClient: 통합 클라이언트 (자동으로 최적의 방법 선택)
    """
    return DataStageAPIClient()

def get_java_sdk_client():
    """
    Java SDK 클라이언트 직접 사용 (고급 사용자용)
    
    Returns:
        DataStageJavaSDKClient: Java SDK 클라이언트
        
    Raises:
        ImportError: Java SDK를 사용할 수 없는 경우
    """
    if not JAVA_SDK_AVAILABLE or DataStageJavaSDKClient is None:
        raise ImportError("Java SDK를 사용할 수 없습니다. JPype1이 설치되어 있는지 확인하세요.")
    return DataStageJavaSDKClient()

__all__ = [
    "DataStageAPIClient",
    "DataStageDesignerClient",
    "DataStageSSHClient",
    "DataStageLocalClient",
    "DependencyAnalyzer",
    "JobModifier",
    "get_datastage_client",
    "get_java_sdk_client",
]

# Java SDK가 사용 가능한 경우에만 추가
if JAVA_SDK_AVAILABLE and DataStageJavaSDKClient is not None:
    __all__.append("DataStageJavaSDKClient")

