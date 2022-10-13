from enum import Enum
from json import dump, load
from typing import Any, Callable, Optional
from logging import getLogger

from cbus import (
    CommadStationError,
    ConfigError,
    ErrorInvalidRequest,
    ErrorLocoAddressTaken,
    ErrorLocoStackFull,
    Frame,
    Header,
    MajorPriority,
    Message,
    MinorPriority,
    OpCode,
)
from mpi_loco import LocoController, LocoDirection
from mpi_util import RepeatTimer, BitField


class SessionStatus(Enum):
    """Enumeration with possible session status."""

    FREE = 0
    OCCUPIED = 1
    DETACHED = 2


class Session:
    """Locomotive session associated with a controller"""

    def __init__(self, session_id: int) -> None:
        """Instance initialization

        Args:
            s_id (int): session identifier
        """
        self._id: int = session_id
        self._loco_address: int = -1
        self._status: SessionStatus = SessionStatus.FREE
        # m = import_module("gpioz_loco")
        # self._loco_ctrl = m.GpiozLocoController(id, dir_pin=10, speed_pin=12)
        self._loco_ctrl: LocoController = LocoController(session_id)
        self._timeout: int = 0
        self._func_map: list[list[Optional[str]]] = [
            ["brake_sw", "half_speed_sw", "inertia_up_sw", "inertia_down_sw", "throttle_sw"],
            [None] * 4,
            [None] * 4,
            [None] * 8,
            [None] * 8,
        ]

    @property
    def session_id(self) -> int:
        """Returns session identifier.

        Returns:
            int: session identifier
        """
        return self._id

    @property
    def loco_control(self) -> LocoController:
        return self._loco_ctrl

    @property
    def loco_address(self) -> int:
        return self._loco_address

    @property
    def is_active(self) -> bool:
        return self._status != SessionStatus.FREE

    @property
    def is_free(self) -> bool:
        return self._status == SessionStatus.FREE

    @property
    def is_detached(self) -> bool:
        return self._status == SessionStatus.DETACHED

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def speed(self) -> int:
        if self.is_active:
            return int(127 * self._loco_ctrl.current_speed)
        return 0

    @speed.setter
    def speed(self, value: int) -> None:
        self._loco_ctrl.target_speed = value / 127.0

    @property
    def direction(self) -> int:
        if self.is_active:
            return self._loco_ctrl.current_direction.value
        return 0

    @direction.setter
    def direction(self, value) -> None:
        self._loco_ctrl.target_direction = LocoDirection(value)

    def allocate(self, loco_address: int):
        self._loco_address = loco_address
        self._timeout = 0
        self._status = SessionStatus.OCCUPIED
        self._loco_ctrl.activate()
        return self

    def detach(self):
        self._status = SessionStatus.DETACHED
        self._timeout = 0

    def free(self) -> None:
        self._loco_ctrl.deactivate()
        self._loco_address = -1
        self._status = SessionStatus.FREE
        self._timeout = 0

    def reset(self) -> None:
        if self.is_active:
            self._loco_ctrl.emergency_stop()
        self._loco_ctrl.deactivate()
        self._status = SessionStatus.FREE

    def stop(self, emergency: bool) -> None:
        if self.is_active:
            if emergency:
                self._loco_ctrl.emergency_stop()
            else:
                self._loco_ctrl.stop()

    def keep_alive(self) -> None:
        self._timeout = 0

    def tick(self) -> int:
        self._timeout += 1
        return self._timeout

    def set_functions(self, func_range, func_byte) -> None:
        funcs = self._func_map[func_range - 1]
        for idx, attr in enumerate(funcs):
            value = bool((func_byte >> idx) & 0x1)
            if attr is not None and hasattr(self._loco_ctrl, attr):
                setattr(self._loco_ctrl, attr, value)

    def get_functions(self, func_range) -> int:
        funcs = self._func_map[func_range - 1]
        value: int = 0
        for idx, attr in enumerate(funcs):
            if attr is not None and hasattr(self._loco_ctrl, attr):
                value |= (int(getattr(self._loco_ctrl, attr)) & 0x01) << idx
        return value


class Stack:
    def __init__(self, size: int, init_id: int) -> None:
        self._initial_session_id = init_id
        self._sessions = [Session(init_id + id) for id in range(size)]
        self._timeouts = {s: False for s in self._sessions}
        self._func_bytes: list[int] = [0] * 5

    @property
    def initial_session_id(self):
        return self._initial_session_id

    @property
    def timeouts(self):
        return self._timeouts

    def reset_all(self) -> None:
        for session in self._sessions:
            session.reset()

    def stop_all(self, emergency: bool) -> None:
        for session in self._sessions:
            session.stop(emergency)

    def stop(self, session_id: int, emergency: bool) -> None:
        session = self.get_session_by_id(session_id)
        session.stop(emergency)

    def request_session(self, loco_address: int, flags: int) -> tuple[Optional[Session], bool]:
        if flags > 3:
            raise ErrorInvalidRequest
        cancel = False
        session: Optional[Session] = self.get_session_by_address(loco_address)
        if session is not None:
            if flags == 0:
                raise ErrorLocoAddressTaken
            elif flags == 1:  # steal
                cancel = True
            elif flags == 2:  # share
                pass
        else:
            session = self.alloc_session(loco_address)
            if session is None:
                raise ErrorLocoStackFull
        return session, cancel

    def keep_alive(self, session_id: int) -> None:
        session = self.get_session_by_id(session_id)
        session.keep_alive()

    def set_speed(self, session_id: int, speed: int):
        session = self.get_session_by_id(session_id)
        session.speed = speed
        session.keep_alive()

    def set_direction(self, session_id: int, direction: int):
        session = self.get_session_by_id(session_id)
        session.direction = direction
        session.keep_alive()

    def alloc_session(self, loco_address: int) -> Optional[Session]:
        for session in self._sessions:
            if session.is_free:
                return session.allocate(loco_address)
        return None

    def query_session(self, session_id: int) -> Optional[Session]:
        session = self.get_session_by_id(session_id)
        return session if session.is_active else None

    def free_session(self, session_id: int) -> None:
        session = self.get_session_by_id(session_id)
        if session.loco_control.is_running:
            session.detach()
        else:
            session.free()

    def get_session_by_id(self, session_id: int) -> Session:
        return self._sessions[session_id - self._initial_session_id]

    def get_session_by_address(self, loco_address: int) -> Optional[Session]:
        for session in self._sessions:
            if session.loco_address == loco_address:
                return session
        return None

    def tick(self, timeout: int) -> None:
        for session in self._sessions:
            if session.is_active and not session.is_detached:
                self._timeouts[session] = session.tick() > timeout
            else:
                self._timeouts[session] = False

    def set_functions(self, session_id: int, func_range: int, func_byte: int) -> None:
        session = self.get_session_by_id(session_id)
        if session.is_active:
            session.set_functions(func_range, func_byte)

    def get_functions(self, session_id: int, func_range: int) -> int:
        session = self.get_session_by_id(session_id)
        value = 0
        if session.is_active:
            value = session.get_functions(func_range)
        return value


class CommandStation:

    NUM_LOCOS = 4
    INITIAL_LOCO_ADDRESS = 10
    INITIAL_SESSION_ID = 16
    TIMEOUT = 5
    TICK = 1
    CS_NUMBER = 0
    EVENT_NUMBER = 0
    EVENT_VARS = 0
    NODE_VARS_NUMBER = 4
    NVMEM_FILE_NAME = "mpi_cs4_nvmem.json"

    def __init__(self) -> None:
        self._logger = getLogger("mpi_cs4")
        self._stack = Stack(self.NUM_LOCOS, self.INITIAL_SESSION_ID)
        self.cb_send: Optional[Callable[[Frame], None]] = None
        self._nvmem: dict[str, dict[str, Any]] = self._load_nvmem()
        self._node_vars_keys = list(self._nvmem["node_vars"])
        self._params: list[int] = self._make_params()
        self._status = BitField(8)
        self._timer = RepeatTimer(self.TICK, self.tick)
        self._timer.start()
        self.reset_status = True

    def __call__(self, frame: Frame) -> list[Frame]:
        return self.handle(frame)

    @property
    def hw_error_status(self) -> bool:
        return self._status[0]

    @hw_error_status.setter
    def hw_error_status(self, value: bool) -> None:
        self._status[0] = value

    @property
    def track_error_status(self):
        return self._status[1]

    @track_error_status.setter
    def track_error_status(self, value: bool) -> None:
        self._status[1] = value

    @property
    def track_status(self) -> bool:
        return self._status[2]

    @track_status.setter
    def track_status(self, value: bool) -> None:
        self._status[2] = value

    @property
    def bus_status(self) -> bool:
        return self._status[3]

    @bus_status.setter
    def bus_status(self, value: bool) -> None:
        self._status[3] = value

    @property
    def emergency_stop_status(self) -> bool:
        return self._status[4]

    @emergency_stop_status.setter
    def emergency_stop_status(self, value: bool) -> None:
        self._status[4] = value

    @property
    def reset_status(self) -> bool:
        return self._status[5]

    @reset_status.setter
    def reset_status(self, value: bool) -> None:
        self._status[5] = value

    @property
    def service_mode_status(self) -> bool:
        return self._status[6]

    @service_mode_status.setter
    def service_mode_status(self, value: bool) -> None:
        self._status[6] = value

    def _start_self_enumeration(self, enum: bool = False) -> None:
        pass

    def _update_self_enumeration(self, can_id: int) -> None:
        pass

    def _load_nvmem(self) -> dict[str, Any]:
        with open(self.NVMEM_FILE_NAME, "rt", encoding="utf-8") as file:
            return load(file)

    def _save_nvmem(self):
        with open(self.NVMEM_FILE_NAME, "wt", encoding="utf-8") as file:
            dump(self._nvmem, file, indent=4)

    def _make_params(self) -> list[int]:
        params = [
            0,
            self.manufacturer_id,
            ord("a") + self.version_minor,
            self.module_id,
            self.EVENT_NUMBER,
            self.EVENT_VARS,
            self.NODE_VARS_NUMBER,
            self.version_major,
            self.node_flags,
        ]
        params[0] = len(params) - 1
        return params

    def get_node_var(self, nv_idx) -> int:
        return self._nvmem["node_vars"][self._node_vars_keys[nv_idx - 1]]

    def set_node_var(self, nv_idx, nv_val) -> None:

        self._nvmem["node_vars"][self._node_vars_keys[nv_idx - 1]] = nv_val
        self._save_nvmem()

    @property
    def can_id(self):
        return self._nvmem["settings"]["CAN_ID"]

    @can_id.setter
    def can_id(self, can_id):
        self._nvmem["settings"]["CAN_ID"] = can_id
        self._save_nvmem()

    @property
    def node_number(self):
        return self._nvmem["settings"]["NODE_NUMBER"]

    @node_number.setter
    def node_number(self, node_number):
        self._nvmem["settings"]["NODE_NUMBER"] = node_number
        self._save_nvmem()

    @property
    def version(self):
        return self._nvmem["settings"]["VERSION"]

    @property
    def manufacturer_id(self) -> int:
        return self._nvmem["settings"]["MANUFACTURER_ID"]

    @property
    def module_name(self):
        return self._nvmem["settings"]["MODULE_NAME"]

    @property
    def module_id(self) -> int:
        return self._nvmem["settings"]["MODULE_ID"]

    @property
    def version_major(self) -> int:
        return int(self.version.split(".")[0])

    @property
    def version_minor(self) -> int:
        return int(self.version.split(".")[1])

    @property
    def version_build(self) -> int:
        return int(self.version.split(".")[2])

    @property
    def node_flags(self) -> int:
        return 0b0100

    def tick(self):
        self._stack.tick(self.TIMEOUT)
        timeouts = self._stack.timeouts
        for session, timeout in timeouts.items():
            if timeout:
                self._logger.error("Session %d TIMEOUT!!", session.session_id)
                header = Header(MajorPriority.NORMAL, MinorPriority.NORMAL, self.can_id)
                msg = Message.make_command_station_error(session.session_id << 8, CommadStationError.SESSION_CANCELLED)
                if self.cb_send and callable(self.cb_send):
                    # pylint: disable=not-callable
                    self.cb_send(Frame(header, msg))
                self._stack.free_session(session.session_id)

    def _request_session(self, loco_address: int, flags: int) -> list[Frame]:
        reply: list[Frame] = list()
        if loco_address in range(self.INITIAL_LOCO_ADDRESS, self.INITIAL_LOCO_ADDRESS + self.NUM_LOCOS):
            try:
                session, do_cancel = self._stack.request_session(loco_address, flags)
                if session is not None:
                    if do_cancel:
                        reply.append(
                            Frame.make_command_station_error(
                                self.can_id, loco_address, CommadStationError.SESSION_CANCELLED
                            )
                        )
                    funcs = [self._stack.get_functions(session.session_id, k) for k in [1, 2, 3]]
                    reply.append(
                        Frame.make_engine_report(
                            self.can_id,
                            session.session_id,
                            session.loco_address,
                            session.speed,
                            session.direction,
                            funcs,
                        )
                    )
            except CommadStationError as exc:
                reply.append(Frame.make_command_station_error(self.can_id, loco_address, exc.code))
        return reply

    def handle(self, frame: Frame) -> list[Frame]:
        reply: list[Frame] = list()
        if frame.is_rtr:
            return [Frame.make_void(self.can_id)]
        if frame.header.can_id == self.can_id:
            self._start_self_enumeration(False)
        else:
            self._update_self_enumeration(frame.header.can_id)
        msg = frame.message
        if msg is None:
            return reply
        opcode = msg.opcode
        if msg.opcode.is_general:
            reply = self.handle_general(msg)
        elif msg.opcode.is_config:
            reply = self.handle_config(msg)
        elif msg.opcode.is_dcc:
            reply = self.handle_dcc(msg)
        elif msg.opcode.is_accessory:
            reply = self.handle_accesory(msg)
        else:
            self._logger.info("Uncategorized opcode %s ignored", opcode.name)
        return reply

    def handle_general(self, msg: Message) -> list[Frame]:
        reply: list[Frame] = list()
        opcode = msg.opcode
        if opcode == OpCode.ARST:
            # ARST - System Reset
            self._stack.reset_all()
        else:
            self._logger.info("General opcode %s ignored", opcode.name)
        return reply

    def handle_config(self, msg: Message) -> list[Frame]:
        reply: list[Frame] = list()
        opcode = msg.opcode
        can_id = self.can_id
        if not msg.has_data:
            if opcode == OpCode.RSTAT:
                reply.append(
                    Frame.make_command_station_report(
                        can_id,
                        self.node_number,
                        self.CS_NUMBER,
                        self._status.as_int,
                        self.version_major,
                        ord("a") + self.version_minor,
                        self.version_build,
                    )
                )
            elif opcode == OpCode.QNN:
                reply.append(
                    Frame.make_query_node_response(
                        self.can_id, self.node_number, self.manufacturer_id, self.module_id, self.node_flags
                    )
                )
            elif opcode == OpCode.RQNP:
                reply.append(Frame.make_node_params(self.can_id, self._params[1:8]))
            elif opcode == OpCode.RQMN:
                reply.append(Frame.make_module_name(self.can_id, self.module_name))
        else:
            (node_number,) = msg.unpack_data("!H")
            if node_number == self.node_number:
                if opcode == OpCode.RQNPN:
                    (par_idx,) = msg.unpack_data("!B", 2)
                    reply.append(Frame.make_parameter(can_id, node_number, par_idx, self._params[par_idx]))
                elif opcode == OpCode.SNN:
                    reply.append(Frame.make_node_number_ack(can_id, node_number))
                elif opcode == OpCode.ENUM:
                    self._start_self_enumeration(True)
                elif opcode == OpCode.CANID:
                    (can_id,) = msg.unpack_data("!B", 2)
                    if 1 <= can_id <= 99:
                        self.can_id = can_id
                        reply.append(Frame.make_node_number_ack(can_id, node_number))
                    else:
                        reply.append(Frame.make_config_error(can_id, node_number, ConfigError.INVALID_EVENT))
                elif opcode == OpCode.RQEVN:
                    reply.append(Frame.make_stored_event_num(can_id, node_number, 0))
                elif opcode == OpCode.NVRD:
                    (nv_idx,) = msg.unpack_data("!B", 2)
                    reply.append(Frame.make_read_node_var(can_id, node_number, nv_idx, self.get_node_var(nv_idx)))
                elif opcode == OpCode.NVSET:
                    nv_idx, nv_val = msg.unpack_data("!2B", 2)
                    if nv_idx > self.NODE_VARS_NUMBER:
                        reply.append(
                            Frame.make_config_error(can_id, node_number, ConfigError.INVALID_NODE_VARIABLE_INDEX)
                        )
                    else:
                        self.set_node_var(nv_idx, nv_val)
                        reply.append(Frame.make_write_ack(can_id, node_number))
                else:
                    self._logger.info("Config opcode %s ignored", opcode.name)
        return reply

    def handle_dcc(self, msg: Message) -> list[Frame]:
        reply: list[Frame] = list()
        opcode = msg.opcode
        if opcode == OpCode.RTOF:
            # RTOF - Request Track Off
            self._stack.stop_all(emergency=True)
        elif opcode == OpCode.RESTP:
            # RESTP - STOP all
            self._stack.stop_all(emergency=True)
            reply.append(Frame.make_emergency_stop(self.can_id))
        elif opcode == OpCode.RTON:
            # RTON - Request Track On
            self.track_status = True
        elif opcode == OpCode.RLOC:
            # RLOC - Request loco session
            (loco_address,) = msg.unpack_data("!H")
            reply = self._request_session(loco_address & 0x3FFF, 0)
        elif opcode == OpCode.GLOC:
            # GLOC - Get loco session (Steal/Share)
            loco_address, flags = msg.unpack_data("!HB")
            loco_address &= 0x3FFF
            reply = self._request_session(loco_address, flags)
        elif opcode == OpCode.KLOC:
            # KLOC - Release loco
            (session_id,) = msg.unpack_data("!B")
            self._stack.free_session(session_id)
        elif opcode == OpCode.QLOC:
            # QLOC - Query loco
            (session_id,) = msg.unpack_data("!B")
            session = self._stack.query_session(session_id)
            if session is not None:
                funcs = [self._stack.get_functions(session.session_id, k) for k in [1, 2, 3]]
                reply.append(
                    Frame.make_engine_report(
                        self.can_id, session_id, session.loco_address, session.speed, session.direction, funcs
                    )
                )
        elif opcode == OpCode.DSPD:
            # DSPD - Set speed & direction
            session_id, speed_dir = msg.unpack_data("!2B")
            speed = speed_dir & 0x7F
            direction = speed_dir >> 7
            if speed == 1:
                self._stack.stop(session_id, emergency=True)
                self._stack.set_direction(session_id, direction)
            else:
                self._stack.set_speed(session_id, speed)
                self._stack.set_direction(session_id, direction)
        elif opcode == OpCode.DFUN:
            session_id, func_range, func_byte = msg.unpack_data("!3B")
            self._stack.set_functions(session_id, func_range, func_byte)
        elif opcode == OpCode.DKEEP:
            # DKEEP - keep alive
            (session_id,) = msg.unpack_data("!B")
            self._stack.keep_alive(session_id)
        else:
            self._logger.info("DCC opcode %s ignored", opcode.name)
        return reply

    def handle_accesory(self, msg: Message) -> list[Frame]:
        reply: list[Frame] = []
        opcode = msg.opcode
        self._logger.info("Accesory opcode %s ignored", opcode.name)
        return reply
