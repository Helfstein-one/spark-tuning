"""Secure sandbox policy and pre-execution validation for experiment runners."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from ..models import RunSpec
from .sanitizer import ArtifactSanitizer, RedactionReport

BASELINE_HOST_ENV_VARS: frozenset[str] = frozenset(
    {
        "PATH",
        "JAVA_HOME",
        "SPARK_HOME",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SYSTEMROOT",
        "PATHEXT",
    }
)

DEFAULT_ALLOWED_ENV_VARS: frozenset[str] = frozenset(
    {
        "SPARK_USER_OPT",
        "SPARK_SQL_OPT",
        "SPARK_CONF_DIR",
        "TZ",
        "LANG",
        "LC_ALL",
    }
)

FORBIDDEN_ENV_VARS: frozenset[str] = frozenset(
    {
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "DYLD_INSERT_LIBRARIES",
        "DYLD_LIBRARY_PATH",
        "BASH_ENV",
        "ENV",
        "IFS",
    }
)

DEFAULT_BLOCKED_SPARK_CONF_KEYS: frozenset[str] = frozenset(
    {
        "spark.authenticate.secret",
        "spark.ssl.keyPassword",
        "spark.ssl.keyStorePassword",
        "spark.hadoop.fs.s3a.access.key",
        "spark.hadoop.fs.s3a.secret.key",
        "spark.hadoop.fs.s3a.session.token",
        "spark.driver.extraLibraryPath",
        "spark.executor.extraLibraryPath",
    }
)

_DANGEROUS_SCRIPT_EXTENSIONS: frozenset[str] = frozenset(
    {".sh", ".bash", ".zsh", ".csh", ".bat", ".cmd", ".ps1"}
)

_DISALLOWED_SPARK_SUBMIT_FLAGS: frozenset[str] = frozenset(
    {
        "--master",
        "--deploy-mode",
        "--class",
        "--conf",
        "-c",
        "--jars",
        "--packages",
        "--repositories",
        "--files",
        "--py-files",
        "--archives",
        "--driver-java-options",
        "--executor-java-options",
        "--proxy-user",
        "--keytab",
        "--principal",
        "--properties-file",
    }
)

_PATH_BEARING_SPARK_CONFS: frozenset[str] = frozenset(
    {
        "spark.jars",
        "spark.files",
        "spark.submit.pyFiles",
        "spark.archives",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
        "spark.local.dir",
    }
)

_CLASSPATH_CONFS: frozenset[str] = frozenset(
    {
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
    }
)

_SENSITIVE_SYSTEM_PREFIXES: tuple[Path, ...] = (
    Path("/etc"),
    Path("/private/etc"),
    Path("/bin"),
    Path("/sbin"),
    Path("/usr/bin"),
    Path("/usr/sbin"),
    Path("/proc"),
    Path("/dev"),
    Path("/sys"),
    Path("/root"),
    Path("/System"),
    Path("/Library"),
    (Path.home() / ".ssh").resolve(strict=False),
    (Path.home() / ".aws").resolve(strict=False),
)

_SHELL_METACHAR_RE = re.compile(r"[\x00\r\n`;|><]|&&|\|\||\$\(|\$\{")
_URI_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*)://([^\s\"']+)")
_PROD_MARKER_RE = re.compile(r"(^|[./:@_-])(prod|production|prd)($|[./:@_-])", re.IGNORECASE)
_LOCAL_MASTER_RE = re.compile(r"^local(?:\[(\*|\d+)\])?$")
_MAIN_CLASS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.$]*$")
_MEM_SPEC_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]b?|b)?\s*$", re.IGNORECASE)
_DANGEROUS_JAVA_OPTS_RE = re.compile(
    r"(?i)(?:-javaagent:|-agentlib:|-Xrunjdwp|-XX:OnOutOfMemoryError|-XX:OnError)"
)


class SandboxViolationError(ValueError):
    """Auditable exception raised when an experiment submission violates SandboxPolicy."""

    def __init__(self, code: str, field: str, detail: str) -> None:
        self.code = str(code)
        self.field = str(field)
        self.detail = ArtifactSanitizer.sanitize_text(str(detail))
        super().__init__(f"[{self.code}] {self.field}: {self.detail}")


@dataclass(frozen=True)
class SanitizedSubmission:
    """Validated and sanitized execution specification produced by SandboxPolicy."""

    run_id: str
    iteration: int
    hypothesis_id: str
    master: str
    deploy_mode: str
    app_resource: str
    app_args: tuple[str, ...]
    main_class: str | None
    spark_conf: dict[str, str]
    env: dict[str, str]
    sanitized_env: dict[str, str]
    timeout_seconds: float
    limits: dict[str, Any]
    redaction_report: RedactionReport


@dataclass(frozen=True)
class SandboxPolicy:
    """Security policy governing SparkSubmitExperimentRunner execution."""

    allowed_roots: tuple[Path, ...] | None = None
    allow_network: bool = False
    allowed_env_vars: frozenset[str] = field(default_factory=lambda: DEFAULT_ALLOWED_ENV_VARS)
    max_driver_memory_mb: int = 2048
    max_executor_memory_mb: int = 4096
    max_cores: int = 4
    max_disk_mb: int = 10240
    timeout_seconds: float = 300.0
    block_prod_masters: bool = True
    allowed_uri_schemes: frozenset[str] = field(default_factory=lambda: frozenset({"file", "local"}))
    blocked_spark_conf_prefixes: tuple[str, ...] = (
        "spark.yarn.",
        "spark.kubernetes.",
        "spark.mesos.",
    )
    blocked_spark_conf_keys: frozenset[str] = field(
        default_factory=lambda: DEFAULT_BLOCKED_SPARK_CONF_KEYS
    )

    def __post_init__(self) -> None:
        if self.allowed_roots is not None:
            if isinstance(self.allowed_roots, (str, Path)):
                roots = (Path(self.allowed_roots).resolve(strict=False),)
            else:
                roots = tuple(Path(p).resolve(strict=False) for p in self.allowed_roots)
            object.__setattr__(self, "allowed_roots", roots)

        object.__setattr__(
            self,
            "allowed_env_vars",
            frozenset(str(v) for v in self.allowed_env_vars),
        )
        object.__setattr__(
            self,
            "allowed_uri_schemes",
            frozenset(str(s).lower() for s in self.allowed_uri_schemes),
        )
        object.__setattr__(
            self,
            "blocked_spark_conf_prefixes",
            tuple(str(p) for p in self.blocked_spark_conf_prefixes),
        )
        object.__setattr__(
            self,
            "blocked_spark_conf_keys",
            frozenset(str(k) for k in self.blocked_spark_conf_keys),
        )

        for name in ("max_driver_memory_mb", "max_executor_memory_mb", "max_cores", "max_disk_mb"):
            val = getattr(self, name)
            if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
                raise ValueError(f"{name} must be a positive integer, got {val!r}")

        timeout = float(self.timeout_seconds)
        if isinstance(self.timeout_seconds, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"timeout_seconds must be a finite positive number, got {self.timeout_seconds!r}")
        object.__setattr__(self, "timeout_seconds", timeout)

    @staticmethod
    def _check_shell_metachars(value: str, field_name: str) -> None:
        if _SHELL_METACHAR_RE.search(value):
            raise SandboxViolationError(
                "SHELL_INJECTION",
                field_name,
                f"Disallowed shell metacharacter or control character in {field_name}: {value!r}",
            )

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> None:
        if not value or any(ch in value for ch in ("\x00", "/", "\\", "\r", "\n")) or ".." in value:
            raise SandboxViolationError(
                "PATH_TRAVERSAL",
                field_name,
                f"Invalid path characters in {field_name}: {value!r}",
            )

    def _validate_uri_or_prod(self, value: str, field_name: str) -> None:
        for match in _URI_RE.finditer(value):
            scheme = match.group(1).lower()
            rest = match.group(2)
            if self.block_prod_masters and _PROD_MARKER_RE.search(rest):
                raise SandboxViolationError(
                    "FORBIDDEN_URI",
                    field_name,
                    f"Production URI is blocked in sandbox: {value}",
                )
            if not self.allow_network and scheme not in {"file", "local"}:
                raise SandboxViolationError(
                    "DISALLOWED_URI_SCHEME",
                    field_name,
                    f"URI scheme {scheme!r} is disallowed when network is disabled: {value}",
                )
            if scheme not in self.allowed_uri_schemes:
                raise SandboxViolationError(
                    "DISALLOWED_URI_SCHEME",
                    field_name,
                    f"URI scheme {scheme!r} is not in allowed_uri_schemes: {value}",
                )

    def _is_symlink_escape(self, candidate: Path) -> bool:
        if candidate.is_symlink():
            return True
        if self.allowed_roots is None:
            return False
        for parent in candidate.parents:
            parent_resolved = parent.resolve(strict=False)
            if parent_resolved in self.allowed_roots:
                break
            if parent.is_symlink() and any(
                parent.parent.resolve(strict=False).is_relative_to(root)
                for root in self.allowed_roots
            ):
                return True
        return False

    def _validate_local_path(self, raw_path: str, field_name: str, check_extension: bool = False) -> Path:
        self._check_shell_metachars(raw_path, field_name)
        self._validate_uri_or_prod(raw_path, field_name)

        local_str = raw_path
        if raw_path.lower().startswith("file://"):
            parsed = urlsplit(raw_path)
            if parsed.netloc not in ("", "localhost"):
                raise SandboxViolationError(
                    "DISALLOWED_URI_SCHEME",
                    field_name,
                    f"Remote file:// authority {parsed.netloc!r} is disallowed",
                )
            local_str = unquote(parsed.path)

        if not local_str:
            raise SandboxViolationError("MISSING_RESOURCE", field_name, f"{field_name} cannot be empty")

        candidate = Path(local_str)
        if ".." in candidate.parts:
            raise SandboxViolationError(
                "PATH_TRAVERSAL",
                field_name,
                f"Path traversal '..' is forbidden in {field_name}: {raw_path}",
            )

        if check_extension and candidate.suffix.lower() in _DANGEROUS_SCRIPT_EXTENSIONS:
            raise SandboxViolationError(
                "RESOURCE_NOT_ALLOWED",
                field_name,
                f"Dangerous executable extension {candidate.suffix!r} is forbidden in {field_name}",
            )

        resolved = candidate.resolve(strict=False)
        if self.allowed_roots is not None:
            if len(self.allowed_roots) == 0:
                raise SandboxViolationError(
                    "RESOURCE_NOT_ALLOWED",
                    field_name,
                    "allowed_roots is empty; no local resources are permitted",
                )
            if not any(resolved.is_relative_to(root) for root in self.allowed_roots):
                code = "SYMLINK_ESCAPE" if self._is_symlink_escape(candidate) else "RESOURCE_NOT_ALLOWED"
                raise SandboxViolationError(
                    code,
                    field_name,
                    f"Resource {raw_path!r} resolves outside allowed_roots",
                )
        else:
            if any(resolved == prefix or resolved.is_relative_to(prefix) for prefix in _SENSITIVE_SYSTEM_PREFIXES):
                raise SandboxViolationError(
                    "RESOURCE_NOT_ALLOWED",
                    field_name,
                    f"Resource {raw_path!r} targets a protected system directory",
                )
        return resolved

    @staticmethod
    def _parse_spark_memory_mb(spec: str, field_name: str) -> float:
        match = _MEM_SPEC_RE.match(spec)
        if not match:
            raise SandboxViolationError(
                "INVALID_MEMORY_SPEC",
                field_name,
                f"Invalid Spark memory specification {spec!r}",
            )
        amount = float(match.group(1))
        unit = (match.group(2) or "m").lower()
        if amount <= 0 or not math.isfinite(amount):
            raise SandboxViolationError(
                "INVALID_MEMORY_SPEC",
                field_name,
                f"Memory specification must be positive, got {spec!r}",
            )
        multipliers = {
            "b": 1.0 / (1024.0 * 1024.0),
            "k": 1.0 / 1024.0,
            "kb": 1.0 / 1024.0,
            "m": 1.0,
            "mb": 1.0,
            "g": 1024.0,
            "gb": 1024.0,
            "t": 1024.0 * 1024.0,
            "tb": 1024.0 * 1024.0,
        }
        return amount * multipliers[unit]

    @staticmethod
    def _parse_positive_int(value: str, field_name: str) -> int:
        try:
            parsed = int(value)
        except ValueError as exc:
            raise SandboxViolationError(
                "INVALID_CORES_SPEC",
                field_name,
                f"Expected positive integer for {field_name}, got {value!r}",
            ) from exc
        if parsed <= 0:
            raise SandboxViolationError(
                "INVALID_CORES_SPEC",
                field_name,
                f"Expected positive integer for {field_name}, got {value!r}",
            )
        return parsed

    @staticmethod
    def _split_conf_paths(key: str, val: str) -> list[str]:
        parts = [p.strip() for p in val.split(",") if p.strip()]
        if key not in _CLASSPATH_CONFS or os.pathsep == ",":
            return parts
        expanded: list[str] = []
        for part in parts:
            if "://" in part:
                expanded.append(part)
            else:
                expanded.extend(sub.strip() for sub in part.split(os.pathsep) if sub.strip())
        return expanded

    def validate_run(
        self,
        spec: RunSpec,
        iteration: int,
        hypothesis: Mapping[str, Any],
    ) -> SanitizedSubmission:
        """Validate a run/hypothesis pair and return a SanitizedSubmission or raise SandboxViolationError."""
        if not isinstance(hypothesis, Mapping):
            raise SandboxViolationError(
                "MISSING_RESOURCE",
                "hypothesis",
                f"Hypothesis must be a mapping, got {type(hypothesis).__name__}",
            )

        run_id = str(spec.run_id)
        self._validate_identifier(run_id, "run_id")

        raw_hyp_id = hypothesis.get("id")
        hypothesis_id = (
            str(raw_hyp_id)
            if raw_hyp_id is not None and str(raw_hyp_id) != ""
            else f"iter_{iteration}"
        )
        self._validate_identifier(hypothesis_id, "hypothesis_id")

        # 1. Validate app_resource / script
        raw_resource = hypothesis.get("app_resource") or hypothesis.get("script")
        if raw_resource is None or not isinstance(raw_resource, (str, Path)) or not str(raw_resource).strip():
            raise SandboxViolationError(
                "MISSING_RESOURCE",
                "app_resource",
                f"Hypothesis {hypothesis_id} requires 'app_resource' or 'script'",
            )
        raw_resource_str = str(raw_resource).strip()
        if "://" in raw_resource_str and not raw_resource_str.lower().startswith("file://"):
            self._check_shell_metachars(raw_resource_str, "app_resource")
            self._validate_uri_or_prod(raw_resource_str, "app_resource")
            canonical_resource = raw_resource_str
        else:
            resolved_resource = self._validate_local_path(
                raw_resource_str,
                "app_resource",
                check_extension=True,
            )
            canonical_resource = str(resolved_resource)

        # 2. Validate master and deploy_mode
        raw_master = hypothesis.get("master")
        master = str(raw_master).strip() if raw_master is not None and str(raw_master).strip() else "local[*]"
        self._check_shell_metachars(master, "master")

        local_match = _LOCAL_MASTER_RE.match(master)
        if local_match is None:
            if self.block_prod_masters or not self.allow_network:
                raise SandboxViolationError(
                    "FORBIDDEN_MASTER",
                    "master",
                    f"Master {master!r} is forbidden in sandbox",
                )
            self._validate_uri_or_prod(master, "master")
        else:
            cores_token = local_match.group(1)
            if cores_token is not None and cores_token != "*":
                requested_cores = int(cores_token)
                if requested_cores < 1 or requested_cores > self.max_cores:
                    raise SandboxViolationError(
                        "RESOURCE_LIMIT_EXCEEDED",
                        "master",
                        f"Requested local cores {requested_cores} exceeds max_cores={self.max_cores}",
                    )

        raw_deploy_mode = hypothesis.get("deploy_mode")
        deploy_mode = (
            str(raw_deploy_mode).strip()
            if raw_deploy_mode is not None and str(raw_deploy_mode).strip()
            else "client"
        )
        self._check_shell_metachars(deploy_mode, "deploy_mode")
        if deploy_mode not in {"client", "cluster"} or (
            (self.block_prod_masters or not self.allow_network) and deploy_mode != "client"
        ):
            raise SandboxViolationError(
                "FORBIDDEN_DEPLOY_MODE",
                "deploy_mode",
                f"Deploy mode {deploy_mode!r} is forbidden in sandbox",
            )

        # 3. Validate main_class and app_args
        raw_main_class = hypothesis.get("main_class")
        main_class: str | None = None
        if raw_main_class is not None and str(raw_main_class).strip():
            main_class = str(raw_main_class).strip()
            self._check_shell_metachars(main_class, "main_class")
            if not _MAIN_CLASS_RE.match(main_class):
                raise SandboxViolationError(
                    "INVALID_CLASS_NAME",
                    "main_class",
                    f"Invalid Java/Scala main_class {main_class!r}",
                )

        raw_args = hypothesis.get("app_args", ())
        if raw_args is None:
            raw_args = ()
        if not isinstance(raw_args, (list, tuple)):
            raise SandboxViolationError(
                "DISALLOWED_ARG_FLAG",
                "app_args",
                f"app_args must be a list or tuple, got {type(raw_args).__name__}",
            )
        clean_args: list[str] = []
        for idx, arg in enumerate(raw_args):
            field_name = f"app_args[{idx}]"
            if not isinstance(arg, (str, int, float, bool)):
                raise SandboxViolationError(
                    "DISALLOWED_ARG_FLAG",
                    field_name,
                    f"Unsupported app_args element type {type(arg).__name__}",
                )
            arg_str = str(arg)
            self._check_shell_metachars(arg_str, field_name)
            self._validate_uri_or_prod(arg_str, field_name)
            if "://" not in arg_str and ".." in Path(arg_str).parts:
                raise SandboxViolationError(
                    "PATH_TRAVERSAL",
                    field_name,
                    f"Path traversal '..' is forbidden in {field_name}: {arg_str}",
                )
            flag_head = arg_str.split("=", 1)[0].strip()
            if flag_head in _DISALLOWED_SPARK_SUBMIT_FLAGS:
                raise SandboxViolationError(
                    "DISALLOWED_ARG_FLAG",
                    field_name,
                    f"spark-submit flag smuggling {flag_head!r} is forbidden in app_args",
                )
            clean_args.append(arg_str)

        # 4. Validate spark_conf, limits, and network isolation
        raw_conf = hypothesis.get("spark_conf", {})
        if raw_conf is None:
            raw_conf = {}
        if not isinstance(raw_conf, Mapping):
            raise SandboxViolationError(
                "BLOCKED_SPARK_CONF",
                "spark_conf",
                f"spark_conf must be a mapping, got {type(raw_conf).__name__}",
            )

        spark_conf: dict[str, str] = {}
        for k, v in raw_conf.items():
            conf_field = f"spark_conf.{k}"
            if not isinstance(k, str) or not k.startswith("spark."):
                raise SandboxViolationError(
                    "BLOCKED_SPARK_CONF",
                    conf_field,
                    f"Disallowed spark_conf key {k!r}; keys must start with 'spark.'",
                )
            self._check_shell_metachars(k, conf_field)
            if k in self.blocked_spark_conf_keys:
                raise SandboxViolationError(
                    "BLOCKED_SPARK_CONF",
                    conf_field,
                    f"Blocked spark_conf key {k!r}",
                )
            if self.block_prod_masters and any(k.startswith(pref) for pref in self.blocked_spark_conf_prefixes):
                raise SandboxViolationError(
                    "BLOCKED_SPARK_CONF",
                    conf_field,
                    f"Cluster-manager spark_conf key {k!r} is blocked in sandbox",
                )
            if not self.allow_network and k in {"spark.jars.packages", "spark.jars.repositories"}:
                raise SandboxViolationError(
                    "NETWORK_ISOLATION_VIOLATION",
                    conf_field,
                    f"{k} is forbidden when allow_network=False",
                )
            if not isinstance(v, (str, int, float, bool)):
                raise SandboxViolationError(
                    "BLOCKED_SPARK_CONF",
                    conf_field,
                    f"Unsupported spark_conf value type {type(v).__name__}",
                )
            v_str = ("true" if v else "false") if isinstance(v, bool) else str(v)
            self._check_shell_metachars(v_str, conf_field)
            if k in {"spark.driver.extraJavaOptions", "spark.executor.extraJavaOptions"}:
                if _DANGEROUS_JAVA_OPTS_RE.search(v_str):
                    raise SandboxViolationError(
                        "BLOCKED_SPARK_CONF",
                        conf_field,
                        f"Dangerous JVM agent/hook option forbidden in {k}",
                    )
            self._validate_uri_or_prod(v_str, conf_field)

            if k in _PATH_BEARING_SPARK_CONFS:
                for entry in self._split_conf_paths(k, v_str):
                    if "://" in entry and not entry.lower().startswith("file://"):
                        self._validate_uri_or_prod(entry, conf_field)
                    else:
                        self._validate_local_path(entry, conf_field, check_extension=False)

            spark_conf[k] = v_str

        # Memory limits
        for mem_key, limit_mb in (
            ("spark.driver.memory", self.max_driver_memory_mb),
            ("spark.driver.memoryOverhead", self.max_driver_memory_mb),
            ("spark.executor.memory", self.max_executor_memory_mb),
            ("spark.executor.memoryOverhead", self.max_executor_memory_mb),
        ):
            if mem_key in spark_conf:
                parsed_mb = self._parse_spark_memory_mb(spark_conf[mem_key], f"spark_conf.{mem_key}")
                if parsed_mb > float(limit_mb):
                    raise SandboxViolationError(
                        "RESOURCE_LIMIT_EXCEEDED",
                        f"spark_conf.{mem_key}",
                        f"{mem_key}={spark_conf[mem_key]!r} ({parsed_mb:.1f}m) exceeds limit {limit_mb}m",
                    )

        if "spark.driver.memory" not in spark_conf:
            spark_conf["spark.driver.memory"] = f"{self.max_driver_memory_mb}m"

        # CPU core limits
        for core_key in ("spark.executor.cores", "spark.driver.cores", "spark.cores.max"):
            if core_key in spark_conf:
                cores_val = self._parse_positive_int(spark_conf[core_key], f"spark_conf.{core_key}")
                if cores_val > self.max_cores:
                    raise SandboxViolationError(
                        "RESOURCE_LIMIT_EXCEEDED",
                        f"spark_conf.{core_key}",
                        f"{core_key}={cores_val} exceeds max_cores={self.max_cores}",
                    )

        if "spark.cores.max" not in spark_conf:
            spark_conf["spark.cores.max"] = str(self.max_cores)

        # Network isolation
        if not self.allow_network:
            for host_key in ("spark.driver.host", "spark.driver.bindAddress"):
                if host_key in spark_conf and spark_conf[host_key] not in {"127.0.0.1", "localhost", "::1"}:
                    raise SandboxViolationError(
                        "NETWORK_ISOLATION_VIOLATION",
                        f"spark_conf.{host_key}",
                        f"Non-loopback {host_key}={spark_conf[host_key]!r} is forbidden when allow_network=False",
                    )
            spark_conf.setdefault("spark.driver.host", "127.0.0.1")
            spark_conf.setdefault("spark.driver.bindAddress", "127.0.0.1")

        # 5. Timeout validation
        raw_timeout = hypothesis.get("timeout_seconds")
        if raw_timeout is not None:
            if (
                isinstance(raw_timeout, bool)
                or not isinstance(raw_timeout, (int, float))
                or not math.isfinite(float(raw_timeout))
                or float(raw_timeout) <= 0
            ):
                raise SandboxViolationError(
                    "INVALID_TIMEOUT",
                    "timeout_seconds",
                    f"timeout_seconds must be a finite positive number, got {raw_timeout!r}",
                )
            effective_timeout = float(raw_timeout)
            if effective_timeout > self.timeout_seconds:
                raise SandboxViolationError(
                    "RESOURCE_LIMIT_EXCEEDED",
                    "timeout_seconds",
                    f"timeout_seconds={effective_timeout} exceeds policy limit {self.timeout_seconds}",
                )
        else:
            effective_timeout = float(self.timeout_seconds)

        if spec.max_runtime_minutes and spec.max_runtime_minutes > 0:
            effective_timeout = min(effective_timeout, float(spec.max_runtime_minutes) * 60.0)

        # 6. Environment allowlist isolation & redaction
        host_allowset = (BASELINE_HOST_ENV_VARS | self.allowed_env_vars) - FORBIDDEN_ENV_VARS
        exec_env: dict[str, str] = {
            k: v for k, v in os.environ.items() if k in host_allowset
        }

        raw_env = hypothesis.get("env", {})
        if raw_env is None:
            raw_env = {}
        if not isinstance(raw_env, Mapping):
            raise SandboxViolationError(
                "INVALID_ENV",
                "env",
                f"env must be a mapping, got {type(raw_env).__name__}",
            )

        accepted_hyp_env: dict[str, str] = {}
        for k, v in raw_env.items():
            if not isinstance(k, str) or not k:
                raise SandboxViolationError(
                    "INVALID_ENV",
                    "env",
                    f"Environment variable names must be non-empty strings, got {k!r}",
                )
            if k in FORBIDDEN_ENV_VARS or k.upper() in FORBIDDEN_ENV_VARS:
                raise SandboxViolationError(
                    "FORBIDDEN_ENV_VAR",
                    f"env.{k}",
                    f"Forbidden loader/shell environment variable {k!r}",
                )
            v_str = str(v)
            if "\x00" in k or "\x00" in v_str:
                raise SandboxViolationError(
                    "INVALID_ENV",
                    f"env.{k}",
                    f"NUL byte is forbidden in environment variable {k!r}",
                )
            if k in self.allowed_env_vars:
                exec_env[k] = v_str
                accepted_hyp_env[k] = v_str

        exec_env["SPARK_RUN_ID"] = run_id
        sanitized_env, redaction_report = ArtifactSanitizer.sanitize_with_report(accepted_hyp_env)

        limits = {
            "max_driver_memory_mb": self.max_driver_memory_mb,
            "max_executor_memory_mb": self.max_executor_memory_mb,
            "max_cores": self.max_cores,
            "max_disk_mb": self.max_disk_mb,
            "timeout_seconds": effective_timeout,
            "allow_network": self.allow_network,
        }

        return SanitizedSubmission(
            run_id=run_id,
            iteration=iteration,
            hypothesis_id=hypothesis_id,
            master=master,
            deploy_mode=deploy_mode,
            app_resource=canonical_resource,
            app_args=tuple(clean_args),
            main_class=main_class,
            spark_conf=spark_conf,
            env=exec_env,
            sanitized_env=sanitized_env,
            timeout_seconds=effective_timeout,
            limits=limits,
            redaction_report=redaction_report,
        )
