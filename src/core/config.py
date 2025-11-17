"""설정 관리 모듈"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class Config:
    """애플리케이션 설정 관리 클래스"""
    
    def __init__(self, config_path: Optional[str] = None, env_path: Optional[str] = None):
        """
        설정 초기화
        
        Args:
            config_path: config.yaml 파일 경로 (기본값: config/config.yaml)
            env_path: .env 파일 경로 (기본값: config/.env)
        """
        # 프로젝트 루트 디렉토리 찾기
        self.root_dir = Path(__file__).parent.parent.parent
        
        # 설정 파일 경로
        if config_path is None:
            config_path = self.root_dir / "config" / "config.yaml"
        else:
            config_path = Path(config_path)
        
        # .env 파일 로드
        if env_path is None:
            env_path = self.root_dir / "config" / ".env"
        else:
            env_path = Path(env_path)
        
        if env_path.exists():
            load_dotenv(env_path)
        
        # YAML 설정 로드
        with open(config_path, 'r', encoding='utf-8') as f:
            self._raw_config = yaml.safe_load(f)
        
        # 환경 변수 치환
        self._config = self._substitute_env_vars(self._raw_config)
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        설정 값에서 환경 변수 치환
        
        Args:
            config: 설정 딕셔너리 또는 값
        
        Returns:
            환경 변수가 치환된 설정
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            # ${VAR_NAME} 형식의 환경 변수 추출
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        else:
            return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        설정 값 조회 (점 표기법 지원)
        
        Args:
            key: 설정 키 (예: "datastage.server_host" 또는 "datastage")
            default: 기본값
        
        Returns:
            설정 값
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_datastage_config(self) -> Dict[str, Any]:
        """DataStage 설정 반환"""
        return self.get("datastage", {})
    
    def get_database_config(self, db_type: str = "mssql") -> Dict[str, Any]:
        """
        데이터베이스 설정 반환
        
        Args:
            db_type: 데이터베이스 타입 ("mssql" 또는 "vertica")
        
        Returns:
            데이터베이스 설정 딕셔너리
        """
        return self.get(f"databases.{db_type}", {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """로깅 설정 반환"""
        return self.get("logging", {})


# 전역 설정 인스턴스
_config_instance: Optional[Config] = None


def get_config(config_path: Optional[str] = None, env_path: Optional[str] = None) -> Config:
    """
    전역 설정 인스턴스 반환 (싱글톤 패턴)
    
    Args:
        config_path: 설정 파일 경로
        env_path: .env 파일 경로
    
    Returns:
        Config 인스턴스
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config(config_path, env_path)
    return _config_instance

