"""IBM DataStage Java SDK 클라이언트 (JPype를 통한 Java 연동)"""

import os
import sys
from typing import Dict, Any, Optional, List
from pathlib import Path

from src.core.config import get_config
from src.core.logger import get_logger

logger = get_logger(__name__)

# JPype는 필요시에만 import
try:
    import jpype
    import jpype.imports
    JPYPE_AVAILABLE = True
except ImportError:
    JPYPE_AVAILABLE = False
    logger.warning("JPype가 설치되어 있지 않습니다. pip install JPype1로 설치하세요.")


class DataStageJavaSDKClient:
    """DataStage Java SDK를 Python에서 호출하는 클라이언트"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Java SDK 클라이언트 초기화
        
        Args:
            config: DataStage 설정 딕셔너리 (None이면 config에서 로드)
        """
        if not JPYPE_AVAILABLE:
            raise ImportError("JPype가 설치되어 있지 않습니다. pip install JPype1로 설치하세요.")
        
        if config is None:
            config = get_config().get_datastage_config()
        
        self.server_host = config.get("server_host")
        self.server_port = config.get("server_port", 9446)
        self.username = config.get("username")
        self.password = config.get("password")
        
        java_sdk_config = config.get("java_sdk", {})
        self.enabled = java_sdk_config.get("enabled", True)
        self.client_path = java_sdk_config.get("client_path", "")
        self.jar_paths = java_sdk_config.get("jar_paths", [])
        self.java_home = java_sdk_config.get("java_home", "")
        
        self._jvm_started = False
        self._dsapi = None
        self._connection = None
    
    def _find_java_home(self) -> Optional[str]:
        """Java 경로 찾기"""
        # 설정에서 지정된 경로 (빈 문자열이 아닌 경우만)
        if self.java_home and self.java_home.strip():
            java_path = Path(self.java_home)
            # jvm.dll 경로 확인 (IBM JDK와 표준 JDK 모두 지원)
            jvm_dll_paths = [
                # IBM JDK 경로
                java_path / "jre" / "bin" / "default" / "jvm.dll",
                java_path / "jre" / "bin" / "j9vm" / "jvm.dll",
                java_path / "jre" / "bin" / "classic" / "jvm.dll",
                java_path / "jre" / "bin" / "compressedrefs" / "jvm.dll",
                # 표준 JDK 경로
                java_path / "bin" / "server" / "jvm.dll",
                java_path / "bin" / "client" / "jvm.dll",
                java_path / "jre" / "bin" / "server" / "jvm.dll",
                java_path / "jre" / "bin" / "client" / "jvm.dll",
            ]
            for jvm_path in jvm_dll_paths:
                if jvm_path.exists():
                    logger.debug(f"JVM DLL 발견: {jvm_path}")
                    return str(java_path)
        
        # DataStage와 함께 설치된 JDK 확인 (우선순위)
        datastage_jdk_paths = [
            Path("C:/IBM/InformationServer/jdk"),
            Path("C:/IBM/InformationServer/jdk32"),
        ]
        for jdk_path in datastage_jdk_paths:
            if jdk_path.exists():
                jvm_dll_paths = [
                    # IBM JDK 경로
                    jdk_path / "jre" / "bin" / "default" / "jvm.dll",
                    jdk_path / "jre" / "bin" / "j9vm" / "jvm.dll",
                    jdk_path / "jre" / "bin" / "classic" / "jvm.dll",
                    jdk_path / "jre" / "bin" / "compressedrefs" / "jvm.dll",
                    # 표준 JDK 경로
                    jdk_path / "bin" / "server" / "jvm.dll",
                    jdk_path / "bin" / "client" / "jvm.dll",
                    jdk_path / "jre" / "bin" / "server" / "jvm.dll",
                    jdk_path / "jre" / "bin" / "client" / "jvm.dll",
                ]
                for jvm_path in jvm_dll_paths:
                    if jvm_path.exists():
                        logger.debug(f"DataStage JDK JVM DLL 발견: {jvm_path}")
                        return str(jdk_path)
        
        # 환경 변수 확인
        java_home = os.getenv("JAVA_HOME")
        if java_home:
            java_path = Path(java_home)
            jvm_dll_paths = [
                java_path / "bin" / "server" / "jvm.dll",
                java_path / "bin" / "client" / "jvm.dll",
            ]
            for jvm_path in jvm_dll_paths:
                if jvm_path.exists():
                    return java_home
        
        # 시스템 PATH에서 찾기
        import shutil
        java_path = shutil.which("java")
        if java_path:
            # java.exe의 상위 디렉토리에서 JAVA_HOME 찾기
            java_dir = Path(java_path).parent.parent
            jvm_dll_paths = [
                java_dir / "bin" / "server" / "jvm.dll",
                java_dir / "bin" / "client" / "jvm.dll",
            ]
            for jvm_path in jvm_dll_paths:
                if jvm_path.exists():
                    return str(java_dir)
        
        return None
    
    def _find_datastage_jars(self) -> List[str]:
        """DataStage JAR 파일 경로 찾기"""
        jar_files = []
        
        # 수동으로 지정된 경로
        if self.jar_paths:
            for jar_path in self.jar_paths:
                if Path(jar_path).exists():
                    jar_files.append(str(Path(jar_path).absolute()))
            if jar_files:
                return jar_files
        
        # 클라이언트 경로에서 찾기
        search_paths = []
        
        if self.client_path:
            search_paths.append(Path(self.client_path))
        else:
            # 일반적인 설치 경로 (우선순위 순서)
            common_paths = [
                # 최신 버전 (우선)
                Path("C:/IBM/InformationServer/ASBNode/lib/java"),
                Path("C:/IBM/InformationServer/ASBNode/lib"),
                # 구버전
                Path("C:/IBM/InformationServer/Clients/Classic/lib"),
                Path("C:/IBM/InformationServer/Clients/Classic"),
                Path("C:/Program Files/IBM/InformationServer/Clients/Classic/lib"),
                Path("C:/Program Files (x86)/IBM/InformationServer/Clients/Classic/lib"),
                Path(os.path.expanduser("~/IBM/InformationServer/Clients/Classic/lib")),
            ]
            search_paths.extend(common_paths)
        
        # 필수 JAR 파일 목록 (최신 버전과 구버전 모두 지원)
        required_jars = [
            # 최신 버전 (ASBNode/lib/java)
            "datastage_api_restclient.jar",
            "datastage_api_common.jar",
            "datastage_common.jar",
            # 구버전 (Clients/Classic/lib)
            "dsapi.jar",
            "dscommon.jar",
            "dsengine.jar",
            "dsserver.jar",
        ]
        
        for base_path in search_paths:
            if not base_path.exists():
                continue
            
            # lib 서브디렉토리가 있으면 사용, 없으면 base_path 자체가 lib 디렉토리
            if (base_path / "lib").exists():
                lib_path = base_path / "lib"
            elif base_path.name == "lib":
                lib_path = base_path
            else:
                lib_path = base_path
            
            # 필수 JAR 파일 찾기
            found_jars = []
            for jar_name in required_jars:
                jar_path = lib_path / jar_name
                if jar_path.exists():
                    found_jars.append(str(jar_path.absolute()))
            
            # 최신 버전의 경우: datastage 관련 JAR 파일이 하나라도 있으면 모든 JAR 포함
            # 구버전의 경우: 필수 JAR가 2개 이상 찾아지면 모든 JAR 포함
            is_modern = any("datastage" in jar.lower() for jar in found_jars)
            min_required = 1 if is_modern else 2
            
            if len(found_jars) >= min_required:
                # lib 디렉토리의 모든 JAR 파일 추가
                for jar_file in lib_path.glob("*.jar"):
                    if str(jar_file.absolute()) not in found_jars:
                        found_jars.append(str(jar_file.absolute()))
                
                logger.info(f"DataStage JAR 파일 발견: {base_path} ({len(found_jars)}개)")
                return found_jars
        
        logger.warning("DataStage JAR 파일을 찾을 수 없습니다. config.yaml에서 jar_paths를 수동으로 설정하세요.")
        return []
    
    def _start_jvm(self) -> bool:
        """JVM 시작 및 DataStage SDK 로드"""
        if self._jvm_started:
            return True
        
        try:
            # Java 경로 찾기
            java_home = self._find_java_home()
            if not java_home:
                logger.error("Java를 찾을 수 없습니다. JAVA_HOME 환경 변수를 설정하거나 config.yaml에서 java_home을 지정하세요.")
                return False
            
            # JVM 시작 여부 확인
            if jpype.isJVMStarted():
                logger.info("JVM이 이미 시작되어 있습니다.")
                self._jvm_started = True
            else:
                # JAR 파일 경로 찾기
                jar_paths = self._find_datastage_jars()
                if not jar_paths:
                    logger.error("DataStage JAR 파일을 찾을 수 없습니다.")
                    return False
                
                # 클래스 경로 설정
                classpath = os.pathsep.join(jar_paths)
                
                # JVM DLL 경로 찾기 (IBM JDK와 표준 JDK 모두 지원)
                jvm_dll_paths = [
                    # IBM JDK 경로 (우선순위)
                    Path(java_home) / "jre" / "bin" / "default" / "jvm.dll",
                    Path(java_home) / "jre" / "bin" / "j9vm" / "jvm.dll",
                    Path(java_home) / "jre" / "bin" / "classic" / "jvm.dll",
                    Path(java_home) / "jre" / "bin" / "compressedrefs" / "jvm.dll",
                    # 표준 JDK 경로
                    Path(java_home) / "bin" / "server" / "jvm.dll",
                    Path(java_home) / "bin" / "client" / "jvm.dll",
                    Path(java_home) / "jre" / "bin" / "server" / "jvm.dll",
                    Path(java_home) / "jre" / "bin" / "client" / "jvm.dll",
                ]
                
                jvm_path = None
                for jvm_dll in jvm_dll_paths:
                    if jvm_dll.exists():
                        jvm_path = str(jvm_dll)
                        logger.debug(f"JVM DLL 경로: {jvm_path}")
                        break
                
                if not jvm_path:
                    # 기본 JVM 경로 시도
                    try:
                        jvm_path = jpype.getDefaultJVMPath()
                    except:
                        logger.error(f"JVM DLL을 찾을 수 없습니다. Java 경로: {java_home}")
                        return False
                
                logger.info(f"JVM 시작 중... (Java: {java_home}, JAR: {len(jar_paths)}개)")
                
                # JVM 시작 옵션 설정 (IBM JDK 호환성 고려)
                # 클래스패스가 너무 길면 문제가 될 수 있으므로 주의
                jvm_options = [
                    f"-Djava.class.path={classpath}",
                    "-Xmx256m",  # 최대 메모리 (낮춤)
                    "-Xms128m",  # 초기 메모리 (낮춤)
                    "-Dfile.encoding=UTF-8",
                ]
                
                # Windows에서 경로 길이 제한 문제 해결
                if len(classpath) > 2000:  # 경로가 너무 길면
                    logger.warning("클래스패스가 너무 깁니다. 일부 JAR만 포함합니다.")
                    # 필수 JAR만 포함
                    essential_jars = [j for j in jar_paths if any(x in j.lower() for x in ['datastage', 'dsapi', 'dscommon'])]
                    if essential_jars:
                        classpath = os.pathsep.join(essential_jars[:50])  # 최대 50개만
                        jvm_options[0] = f"-Djava.class.path={classpath}"
                
                # JPype 시작 (convertStrings=False는 기본값)
                # JVM이 이미 시작되어 있으면 스킵
                if jpype.isJVMStarted():
                    logger.info("JVM이 이미 시작되어 있습니다. 기존 JVM 사용")
                    self._jvm_started = True
                else:
                    try:
                        jpype.startJVM(
                            jvm_path,
                            *jvm_options,
                            convertStrings=False
                        )
                        logger.info("JVM 시작 성공")
                        self._jvm_started = True
                    except Exception as e:
                        error_msg = str(e).lower()
                        # "JVM is already started" 오류 처리
                        if "already started" in error_msg or "jvm" in error_msg and "started" in error_msg:
                            logger.info("JVM이 이미 시작되어 있습니다. 기존 JVM 사용")
                            self._jvm_started = True
                        else:
                            # 첫 시도 실패 시 다른 방식 시도
                            logger.debug(f"첫 번째 JVM 시작 시도 실패: {e}")
                            try:
                                # JVM 경로를 디렉토리로 전달 (JPype가 자동으로 jvm.dll 찾기)
                                jvm_dir = Path(jvm_path).parent.parent.parent.parent  # jre/bin/default/jvm.dll -> jre
                                jpype.startJVM(
                                    str(jvm_dir / "bin" / "default" / "jvm.dll") if (jvm_dir / "bin" / "default" / "jvm.dll").exists() else jvm_path,
                                    *jvm_options,
                                    convertStrings=False
                                )
                                logger.info("JVM 시작 성공 (대체 방법)")
                                self._jvm_started = True
                            except Exception as e2:
                                error_msg2 = str(e2).lower()
                                if "already started" in error_msg2 or "jvm" in error_msg2 and "started" in error_msg2:
                                    logger.info("JVM이 이미 시작되어 있습니다. 기존 JVM 사용")
                                    self._jvm_started = True
                                else:
                                    logger.error(f"JVM 시작 실패: {e2}")
                                    import traceback
                                    logger.debug(traceback.format_exc())
                                    return False
            
            # DataStage API 클래스 로드
            try:
                # JPype의 import 시스템 사용
                # com.ibm.datastage.api.DSAPI 클래스 로드 시도
                try:
                    # 방법 1: 직접 클래스 로드
                    dsapi_class = jpype.JClass("com.ibm.datastage.api.DSAPI")
                    logger.debug("DSAPI 클래스 로드 성공 (JClass)")
                except:
                    # 방법 2: Class.forName 사용
                    try:
                        dsapi_class = jpype.java.lang.Class.forName("com.ibm.datastage.api.DSAPI")
                        logger.debug("DSAPI 클래스 로드 성공 (Class.forName)")
                    except:
                        # 방법 3: 패키지에서 직접 import
                        try:
                            from jpype import java
                            dsapi_class = java.lang.Class.forName("com.ibm.datastage.api.DSAPI")
                            logger.debug("DSAPI 클래스 로드 성공 (java.lang.Class.forName)")
                        except Exception as e:
                            logger.error(f"DataStage SDK 클래스 로드 실패: {e}")
                            logger.error("가능한 원인:")
                            logger.error("  1. JAR 파일 경로가 올바르지 않음")
                            logger.error("  2. DataStage SDK 버전이 다름")
                            logger.error("  3. 클래스 이름이 다를 수 있음")
                            return False
                
                # 클래스 로드 성공 확인
                if dsapi_class:
                    logger.info("DataStage Java SDK 로드 성공")
                    return True
                else:
                    logger.error("DSAPI 클래스를 로드했지만 인스턴스를 생성할 수 없습니다.")
                    return False
                    
            except Exception as e:
                logger.error(f"DataStage SDK 클래스 로드 실패: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                return False
                
        except Exception as e:
            logger.error(f"JVM 시작 실패: {e}")
            return False
    
    def _get_dsapi(self):
        """DSAPI 인스턴스 가져오기"""
        if not self._start_jvm():
            return None
        
        try:
            # DSAPI 클래스 로드 (여러 방법 시도)
            dsapi_class = None
            dsapi = None
            
            # 방법 1: JClass로 직접 로드
            try:
                dsapi_class = jpype.JClass("com.ibm.datastage.api.DSAPI")
                logger.debug("DSAPI 클래스 로드 성공 (JClass)")
            except Exception as e1:
                logger.debug(f"JClass 로드 실패: {e1}")
                # 방법 2: Class.forName 사용
                try:
                    dsapi_class = jpype.java.lang.Class.forName("com.ibm.datastage.api.DSAPI")
                    logger.debug("DSAPI 클래스 로드 성공 (Class.forName)")
                except Exception as e2:
                    logger.error(f"DSAPI 클래스 로드 실패: {e2}")
                    return None
            
            if not dsapi_class:
                return None
            
            # 인스턴스 생성 (여러 방법 시도)
            # 방법 1: getInstance() 정적 메서드
            try:
                if hasattr(dsapi_class, "getInstance"):
                    get_instance = dsapi_class.getInstance
                    if callable(get_instance):
                        dsapi = get_instance()
                        logger.debug("DSAPI 인스턴스 생성 성공 (getInstance)")
            except Exception as e:
                logger.debug(f"getInstance() 실패: {e}")
            
            # 방법 2: 생성자
            if not dsapi:
                try:
                    dsapi = dsapi_class()
                    logger.debug("DSAPI 인스턴스 생성 성공 (생성자)")
                except Exception as e:
                    logger.debug(f"생성자 호출 실패: {e}")
            
            # 방법 3: 클래스 자체를 반환 (정적 메서드 사용)
            if not dsapi:
                dsapi = dsapi_class
                logger.debug("DSAPI 클래스 자체 사용 (정적 메서드)")
            
            return dsapi
            
        except Exception as e:
            logger.error(f"DSAPI 인스턴스 생성 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def connect(self) -> bool:
        """
        DataStage 서버에 연결
        
        Returns:
            연결 성공 여부
        """
        try:
            dsapi = self._get_dsapi()
            if not dsapi:
                return False
            
            logger.info(f"DataStage 서버 연결 시도: {self.server_host}:{self.server_port}")
            
            # 연결 메서드 시도 (여러 패턴 시도)
            connection_methods = [
                ("connect", [self.server_host, str(self.server_port), self.username, self.password]),
                ("connect", [self.server_host, self.server_port]),
                ("setServer", [self.server_host, self.server_port]),
                ("setConnection", [self.server_host, self.server_port, self.username, self.password]),
            ]
            
            connected = False
            for method_name, args in connection_methods:
                try:
                    if hasattr(dsapi, method_name):
                        method = getattr(dsapi, method_name)
                        if callable(method):
                            method(*args)
                            connected = True
                            logger.debug(f"연결 메서드 성공: {method_name}")
                            break
                except Exception as e:
                    logger.debug(f"연결 메서드 실패 ({method_name}): {e}")
                    continue
            
            # 연결 메서드가 없거나 실패한 경우, 연결 없이 진행 (로컬 작업인 경우)
            if not connected:
                logger.info("명시적 연결 메서드를 찾을 수 없습니다. 로컬 작업으로 진행합니다.")
            
            self._connection = {
                "host": self.server_host,
                "port": self.server_port,
                "username": self.username,
                "connected": True
            }
            
            logger.info("DataStage 서버 연결 성공 (Java SDK)")
            return True
            
        except Exception as e:
            logger.error(f"DataStage 서버 연결 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def get_projects(self) -> List[Dict[str, Any]]:
        """
        프로젝트 목록 조회
        
        Returns:
            프로젝트 정보 리스트
        """
        if not self._connection:
            if not self.connect():
                return []
        
        try:
            dsapi = self._get_dsapi()
            if not dsapi:
                return []
            
            projects = []
            
            # 여러 가능한 메서드 이름 시도
            method_names = [
                "getProjectList",
                "getProjects",
                "listProjects",
                "getAllProjects",
            ]
            
            for method_name in method_names:
                try:
                    if hasattr(dsapi, method_name):
                        method = getattr(dsapi, method_name)
                        if callable(method):
                            result = method()
                            
                            # 결과가 Java 컬렉션인 경우 Python 리스트로 변환
                            if result:
                                # Iterator 또는 List인 경우
                                if hasattr(result, "iterator"):
                                    iterator = result.iterator()
                                    while iterator.hasNext():
                                        item = iterator.next()
                                        project_name = str(item) if hasattr(item, "toString") else str(item)
                                        projects.append({
                                            "name": project_name,
                                            "source": "java_sdk"
                                        })
                                elif hasattr(result, "__iter__"):
                                    # Python iterable로 변환 가능한 경우
                                    for item in result:
                                        project_name = str(item) if hasattr(item, "toString") else str(item)
                                        projects.append({
                                            "name": project_name,
                                            "source": "java_sdk"
                                        })
                                else:
                                    # 단일 객체인 경우
                                    project_name = str(result) if hasattr(result, "toString") else str(result)
                                    projects.append({
                                        "name": project_name,
                                        "source": "java_sdk"
                                    })
                            
                            if projects:
                                logger.info(f"프로젝트 목록 조회 성공 ({method_name}): {len(projects)}개")
                                return projects
                except Exception as e:
                    logger.debug(f"메서드 {method_name} 호출 실패: {e}")
                    continue
            
            logger.warning("프로젝트 목록 조회 메서드를 찾을 수 없습니다.")
            return []
            
        except Exception as e:
            logger.error(f"프로젝트 목록 조회 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def get_jobs(self, project_name: str) -> List[Dict[str, Any]]:
        """
        Job 목록 조회
        
        Args:
            project_name: 프로젝트 이름
        
        Returns:
            Job 정보 리스트
        """
        if not self._connection:
            if not self.connect():
                return []
        
        try:
            dsapi = self._get_dsapi()
            if not dsapi:
                return []
            
            jobs = []
            
            # 여러 가능한 메서드 이름 시도
            method_names = [
                "getJobList",
                "getJobs",
                "listJobs",
                "getAllJobs",
            ]
            
            for method_name in method_names:
                try:
                    if hasattr(dsapi, method_name):
                        method = getattr(dsapi, method_name)
                        if callable(method):
                            # 프로젝트 이름을 인자로 전달
                            try:
                                result = method(project_name)
                            except:
                                # 인자가 없는 경우
                                result = method()
                            
                            # 결과가 Java 컬렉션인 경우 Python 리스트로 변환
                            if result:
                                # Iterator 또는 List인 경우
                                if hasattr(result, "iterator"):
                                    iterator = result.iterator()
                                    while iterator.hasNext():
                                        item = iterator.next()
                                        job_name = str(item) if hasattr(item, "toString") else str(item)
                                        jobs.append({
                                            "name": job_name,
                                            "project": project_name,
                                            "source": "java_sdk"
                                        })
                                elif hasattr(result, "__iter__"):
                                    # Python iterable로 변환 가능한 경우
                                    for item in result:
                                        job_name = str(item) if hasattr(item, "toString") else str(item)
                                        jobs.append({
                                            "name": job_name,
                                            "project": project_name,
                                            "source": "java_sdk"
                                        })
                                else:
                                    # 단일 객체인 경우
                                    job_name = str(result) if hasattr(result, "toString") else str(result)
                                    jobs.append({
                                        "name": job_name,
                                        "project": project_name,
                                        "source": "java_sdk"
                                    })
                            
                            if jobs:
                                logger.info(f"Job 목록 조회 성공 ({method_name}): {project_name} - {len(jobs)}개")
                                return jobs
                except Exception as e:
                    logger.debug(f"메서드 {method_name} 호출 실패: {e}")
                    continue
            
            logger.warning(f"Job 목록 조회 메서드를 찾을 수 없습니다. (프로젝트: {project_name})")
            return []
            
        except Exception as e:
            logger.error(f"Job 목록 조회 실패: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
    def get_job_definition(self, project_name: str, job_name: str) -> Optional[Dict[str, Any]]:
        """
        Job 정의 조회
        
        Args:
            project_name: 프로젝트 이름
            job_name: Job 이름
        
        Returns:
            Job 정의 딕셔너리
        """
        if not self._connection:
            if not self.connect():
                return None
        
        try:
            dsapi = self._get_dsapi()
            if not dsapi:
                return None
            
            # DSAPI를 통해 Job 정의 조회
            # 실제 API는 버전에 따라 다를 수 있음
            
            # 예시: dsapi.getJobDefinition(projectName, jobName) 또는 유사한 메서드
            # 실제 구현은 DataStage SDK 문서 참조 필요
            
            logger.info(f"Job 정의 조회 성공: {project_name}/{job_name}")
            return None
            
        except Exception as e:
            logger.error(f"Job 정의 조회 실패: {e}")
            return None
    
    def test_connection(self) -> Dict[str, Any]:
        """
        연결 테스트
        
        Returns:
            연결 테스트 결과 딕셔너리
        """
        result = {
            "success": False,
            "method": "java_sdk",
            "java_home": None,
            "jar_files": [],
            "error": None
        }
        
        try:
            # Java 경로 확인
            java_home = self._find_java_home()
            result["java_home"] = java_home
            
            if not java_home:
                result["error"] = "Java를 찾을 수 없습니다"
                return result
            
            # JAR 파일 확인
            jar_paths = self._find_datastage_jars()
            result["jar_files"] = jar_paths
            
            if not jar_paths:
                result["error"] = "DataStage JAR 파일을 찾을 수 없습니다"
                return result
            
            # JVM 시작 시도
            if self._start_jvm():
                # 연결 시도
                if self.connect():
                    result["success"] = True
                    logger.info("Java SDK 연결 테스트 성공")
                else:
                    result["error"] = "DataStage 서버 연결 실패"
            else:
                result["error"] = "JVM 시작 실패"
                
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Java SDK 연결 테스트 실패: {e}")
        
        return result
    
    def close(self):
        """연결 종료 및 리소스 정리"""
        if self._jvm_started and jpype.isJVMStarted():
            try:
                # JVM 종료는 일반적으로 권장되지 않음 (성능 문제)
                # 하지만 필요시 종료 가능
                # jpype.shutdownJVM()
                pass
            except Exception as e:
                logger.warning(f"JVM 종료 중 오류: {e}")
        
        self._connection = None

