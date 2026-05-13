# 고객정보 테이블 담당자 집계 배치 개발 요구사항

## 목적
기존 개인정보 제공 배치에 테이블 담당자 정보를 추가한다. `wam_cust_info`는 컬럼 단위로 고객정보가 등록되어 있어 테이블 정보가 중복되므로, 먼저 고객정보 테이블 distinct 결과를 집계 테이블에 적재한 뒤 담당자를 최대한 매핑한다. 개발은 Java/MariaDB 기준으로 작성하며, 로컬 실행 검증은 하지 않아도 된다. 개발된 소스는 내부 반입 후 compile하여 기존 서비스 배치에 붙일 예정이다.

## 대상 테이블
| 테이블 | 역할 |
|---|---|
| `wam_cust_info` | 고객정보 원장. DB + Schema + Table + Column 단위 |
| `wam_ddl_tbl` | DDL 테이블 원장 |
| `wam_pdm_tbl` | 모델 테이블 |
| `waa_subj` | 주제영역/모듈코드 및 담당자 정보 |
| `wat_cust_tbl_owner` | 신규 담당자 집계 테이블 |

## 핵심 요구사항
1. 목표는 고객정보 테이블에 담당자를 최대한 많이 매핑하는 것이다.
2. `wam_cust_info`에서 고객정보 테이블 distinct 결과를 먼저 집계 테이블에 insert한다.
3. `wam_cust_info.ddl_tbl_id`는 사용하지 않는다.
4. 1순위 매핑은 테이블명 앞자리 3자리와 `waa_subj`의 모듈코드를 매칭해서 담당자를 찾는다.
5. 단, `waa_subj`의 모듈코드 중복 건은 제외하고, 중복 없이 단독으로 존재하는 모듈코드만 사용한다.
6. 1순위로 담당자가 매핑된 건은 2순위 매핑으로 덮어쓰지 않는다.
7. 1순위로 담당자 매핑이 안 된 건만 `wam_ddl_tbl` → `wam_pdm_tbl` → `waa_subj` 경로로 담당자를 보강 매핑한다.
8. 담당자 매핑 실패는 배치 실패가 아니라 상태값으로 관리한다.
9. 기존 개인정보 제공 배치의 결과 건수는 변경되면 안 되며, 담당자 정보만 `LEFT JOIN`으로 추가한다.

## 처리 흐름
```text
1. 집계 테이블 TRUNCATE
2. wam_cust_info에서 DB + Schema + Table distinct 결과를 집계 테이블에 INSERT
3. 테이블명 앞 3자리(tbl_prefix_cd)를 생성
4. waa_subj에서 중복 없는 모듈코드만 추출
5. tbl_prefix_cd = module_cd 기준으로 1차 담당자 매핑
6. 1차 미매핑 건만 wam_ddl_tbl에서 DDL 정보 매핑
7. DDL 정보 기준으로 wam_pdm_tbl 모델 정보 매핑
8. 모델의 subj_id 기준으로 waa_subj 담당자 2차 보강 매핑
9. 최종 미매핑 건은 UNMATCHED 상태로 남김
10. 기존 개인정보 제공 배치에서 집계 테이블 LEFT JOIN

신규테이블집계
CREATE TABLE wat_cust_tbl_owner (
    db_nm              VARCHAR(100) NOT NULL,
    schema_id          VARCHAR(100) NULL,
    schema_nm          VARCHAR(100) NOT NULL,
    ddl_tbl_pnm        VARCHAR(200) NOT NULL,
    tbl_prefix_cd      VARCHAR(3)   NULL,
    ddl_tbl_id         VARCHAR(100) NULL,
    pdm_tbl_id         VARCHAR(100) NULL,
    ddl_pdm_id         VARCHAR(100) NULL,
    subj_id            VARCHAR(100) NULL,
    tbl_owner_id       VARCHAR(100) NULL,
    tbl_owner_nm       VARCHAR(100) NULL,
    created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (db_nm, schema_nm, ddl_tbl_pnm)
);
