import streamlit as st
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
import networkx as nx
from streamlit_agraph import agraph, Node, Edge, Config

from src.datastage.dependency_analyzer import DependencyAnalyzer
from src.datastage.dependency_graph import DependencyGraph
from src.core.config import get_config

@st.cache_resource
def get_dependency_analyzer(export_dir: str) -> DependencyAnalyzer:
    """
    DependencyAnalyzer 인스턴스를 생성하고 캐싱합니다.
    """
    return DependencyAnalyzer(export_directory=export_dir, use_cache=True, resolve_parameters=True)

def get_graph_config() -> Config:
    """
    streamlit-agraph 설정 반환
    """
    return Config(
        width=800,
        height=600,
        directed=True, 
        physics=True, 
        hierarchical=False,
        nodeHighlightBehavior=True, 
        highlightColor="#F7A7A6",
        collapsible=False
    )

def convert_to_agraph(
    dependency_graph: DependencyGraph, 
    start_node: str, 
    max_level: int = 2,
    direction: str = "all"
) -> Tuple[List[Node], List[Edge]]:
    """
    DependencyGraph 데이터를 streamlit-agraph용 Node/Edge 리스트로 변환
    """
    nodes = []
    edges = []
    added_nodes = set()

    # 1. NetworkX 그래프 생성 (탐색용)
    # DependencyGraph 자체에 탐색 기능이 있지만, 시각화를 위해 부분 그래프를 추출해야 함
    # 여기서는 간단히 BFS로 탐색하여 노드 수집
    
    queue = [(start_node, 0)]
    added_nodes.add(start_node)
    
    # 시작 노드 추가
    nodes.append(Node(id=start_node, label=start_node, size=25, color="#FF5733")) # 시작 노드 강조

    while queue:
        current_node, level = queue.pop(0)
        
        if level >= max_level:
            continue

        # 현재 노드와 연결된 이웃 노드 찾기
        # DependencyGraph 구조상 Job -> Table, Table -> Job 관계를 추적해야 함
        
        # Job인 경우
        if current_node in dependency_graph.job_to_sources:
            # Sources (Table)
            if direction in ["all", "upstream"]:
                for source in dependency_graph.job_to_sources[current_node]:
                    if source not in added_nodes:
                        nodes.append(Node(id=source, label=source, size=15, color="#33FF57")) # Table
                        added_nodes.add(source)
                        queue.append((source, level + 1))
                    edges.append(Edge(source=source, target=current_node, label="reads"))
            
            # Targets (Table)
            if direction in ["all", "downstream"]:
                for target in dependency_graph.job_to_targets[current_node]:
                    if target not in added_nodes:
                        nodes.append(Node(id=target, label=target, size=15, color="#33FF57")) # Table
                        added_nodes.add(target)
                        queue.append((target, level + 1))
                    edges.append(Edge(source=current_node, target=target, label="writes"))

        # Table인 경우 (역으로 추적)
        else:
            # Table -> Job (이 테이블을 읽는 Job - Downstream)
            if direction in ["all", "downstream"]:
                direct_jobs = dependency_graph.get_direct_impact_jobs(current_node)
                for job in direct_jobs:
                    if job not in added_nodes:
                        nodes.append(Node(id=job, label=job, size=20, color="#3357FF")) # Job
                        added_nodes.add(job)
                        queue.append((job, level + 1))
                    edges.append(Edge(source=current_node, target=job, label="reads"))
            
            # Job -> Table (이 테이블을 쓰는 Job - Upstream)
            if direction in ["all", "upstream"]:
                # 이건 DependencyGraph에 직접적인 역참조가 없으면 전체 검색 필요할 수도 있음
                # 하지만 job_to_targets를 뒤지면 됨. 성능 이슈 가능성 있음.
                # 일단 생략하거나 필요시 구현. 여기서는 Downstream 위주로 구현.
                pass

    return nodes, edges
