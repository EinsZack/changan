from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from ..models.cluster import (
    ClusterCreateRequest,
    ClusterEvent,
    ClusterEventCategory,
    ClusterListResponse,
    ClusterNodeListResponse,
    ClusterUpdateRequest,
    ClusterView,
)
from ..models.node import NodeRecord
from ..services.cluster_service import (
    ClusterAlreadyExistsError,
    ClusterNotEmptyError,
    ClusterNotFoundError,
    ClusterService,
    utc_now,
)
from ..services.node_service import NodeNotFoundError
from ..stores.base import ClusterConflictError


router = APIRouter(prefix="/api/v1/clusters", tags=["clusters"])


def get_cluster_service(request: Request) -> ClusterService:
    return request.app.state.cluster_service


ClusterServiceDependency = Annotated[ClusterService, Depends(get_cluster_service)]


def _translate_error(exc: Exception) -> HTTPException:
    if isinstance(exc, (ClusterNotFoundError, NodeNotFoundError)):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, ClusterNotEmptyError):
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": exc.code, "message": str(exc)},
        )
    if isinstance(
        exc, (ClusterAlreadyExistsError, ClusterConflictError)
    ):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="unexpected error",
    )


@router.post("", response_model=ClusterView, status_code=status.HTTP_201_CREATED)
def create_cluster(
    payload: ClusterCreateRequest, service: ClusterServiceDependency
) -> ClusterView:
    try:
        return service.create_cluster(payload)
    except (ClusterAlreadyExistsError, NodeNotFoundError, ClusterConflictError) as exc:
        raise _translate_error(exc) from exc


@router.get("", response_model=ClusterListResponse)
def list_clusters(service: ClusterServiceDependency) -> ClusterListResponse:
    items = service.list_clusters()
    return ClusterListResponse(items=items, total=len(items), updated_at=utc_now())


@router.get("/{cluster_id}/nodes", response_model=ClusterNodeListResponse)
def list_cluster_nodes(
    cluster_id: str, service: ClusterServiceDependency
) -> ClusterNodeListResponse:
    try:
        items = service.list_nodes(cluster_id)
    except ClusterNotFoundError as exc:
        raise _translate_error(exc) from exc
    return ClusterNodeListResponse(items=items, total=len(items), updated_at=utc_now())


@router.get("/{cluster_id}/events", response_model=list[ClusterEvent])
def list_cluster_events(
    cluster_id: str,
    service: ClusterServiceDependency,
    category: ClusterEventCategory | None = None,
    limit: int = Query(50, ge=1, le=100),
) -> list[ClusterEvent]:
    try:
        return service.list_events(cluster_id, category, limit)
    except ClusterNotFoundError as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/{cluster_id}/nodes/{node_id}",
    response_model=NodeRecord,
)
def assign_cluster_node(
    cluster_id: str,
    node_id: UUID,
    service: ClusterServiceDependency,
) -> NodeRecord:
    try:
        return service.assign_node(cluster_id, node_id)
    except (ClusterNotFoundError, NodeNotFoundError, ClusterConflictError) as exc:
        raise _translate_error(exc) from exc


@router.post(
    "/{cluster_id}/nodes/{node_id}/move",
    response_model=NodeRecord,
)
def move_cluster_node(
    cluster_id: str,
    node_id: UUID,
    service: ClusterServiceDependency,
) -> NodeRecord:
    try:
        return service.move_node(node_id, cluster_id)
    except (ClusterNotFoundError, NodeNotFoundError, ClusterConflictError) as exc:
        raise _translate_error(exc) from exc


@router.delete(
    "/{cluster_id}/nodes/{node_id}",
    response_model=NodeRecord,
)
def unassign_cluster_node(
    cluster_id: str,
    node_id: UUID,
    service: ClusterServiceDependency,
) -> NodeRecord:
    try:
        return service.unassign_node(node_id, expected_cluster_id=cluster_id)
    except (ClusterNotFoundError, NodeNotFoundError, ClusterConflictError) as exc:
        raise _translate_error(exc) from exc


@router.get("/{cluster_id}", response_model=ClusterView)
def get_cluster(cluster_id: str, service: ClusterServiceDependency) -> ClusterView:
    try:
        return service.get_cluster(cluster_id)
    except ClusterNotFoundError as exc:
        raise _translate_error(exc) from exc


@router.patch("/{cluster_id}", response_model=ClusterView)
def update_cluster(
    cluster_id: str,
    payload: ClusterUpdateRequest,
    service: ClusterServiceDependency,
) -> ClusterView:
    try:
        return service.update_cluster(cluster_id, payload)
    except ClusterNotFoundError as exc:
        raise _translate_error(exc) from exc


@router.delete(
    "/{cluster_id}",
    response_model=None,
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_cluster(cluster_id: str, service: ClusterServiceDependency) -> Response:
    try:
        service.delete_cluster(cluster_id)
    except (ClusterNotFoundError, ClusterNotEmptyError) as exc:
        raise _translate_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
