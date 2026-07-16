defmodule Eqmi do
  use GenServer
  require Logger

  @moduledoc """
  Documentation for `Eqmi`.
  """

  # Service and message-type id lookups live in Eqmi.Types (the single
  # source of truth used for both header encode/decode and CID allocation);
  # these delegate so callers keep a stable Eqmi.* API.
  defdelegate message_type_id(msg_name), to: Eqmi.Types, as: :message_id
  defdelegate service_type_id(msg_name), to: Eqmi.Types, as: :service_id

  def qmux_message(header, payload) do
    if_type = <<1::little-unsigned-integer-size(8)>>

    [if_type, header, payload]
    |> :erlang.list_to_binary()
  end

  defmodule ClientState do
    @moduledoc false
    defstruct type: nil,
              current_tx: 0,
              id: 0,
              pid: nil
  end

  @doc """
  Open (or fetch) a QMI device. Options:

    * `:transport` - `:raw` (default, cdc-wdm QMUX) or `:mbim`
      (QMI-over-MBIM passthrough for a modem in MBIM mode)
  """
  @spec device(String.t(), keyword()) :: {:ok, reference()} | {:error, term()}
  def device(path, opts \\ []) do
    # the MBIM transport does a CLOSE/OPEN handshake in init, so allow time
    GenServer.call(__MODULE__, {:get_device, path, opts}, 15_000)
  end

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop(pid, reason \\ :shutdown, timeout \\ :infinity) do
    GenServer.stop(pid, reason, timeout)
  end

  def client(dev_ref, type) do
    with {:ok, path} <- GenServer.call(__MODULE__, {:get_path, dev_ref}) do
      Eqmi.Device.client(path, type)
    else
      err -> err
    end
  end

  def release_client(dev_ref, client_ref) do
    with {:ok, path} <- GenServer.call(__MODULE__, {:get_path, dev_ref}) do
      Eqmi.Device.release_client(path, client_ref)
    else
      err -> err
    end
  end

  def send_message(dev_ref, client_ref, msg) do
    with {:ok, path} <- GenServer.call(__MODULE__, {:get_path, dev_ref}) do
      Eqmi.Device.send_message(path, client_ref, msg)
    else
      err -> err
    end
  end

  @doc """
  Send a request and wait for its correlated response.

  Returns `{:ok, response}` or `{:error, reason}`. Unlike `send_message/3`,
  the reply is matched by transaction id so overlapping requests on one
  client don't get crossed. Indications are still delivered to the owner's
  mailbox as `{:qmux, msg}`.
  """
  def call(dev_ref, client_ref, msg, timeout \\ 5_000) do
    with {:ok, path} <- GenServer.call(__MODULE__, {:get_path, dev_ref}) do
      Eqmi.Device.call(path, client_ref, msg, timeout)
    else
      err -> err
    end
  end

  def init(_) do
    {:ok, %{devices: %{}, refs: %{}}}
  end

  def handle_call({:get_device, device_path, opts}, _from, state) do
    path = device_path |> String.trim()

    case Map.get(state.devices, path) do
      nil ->
        spec = {Eqmi.Device, [device: path] ++ opts}

        case DynamicSupervisor.start_child(Eqmi.DynamicSupervisor, spec) do
          {:ok, _pid} ->
            ref = make_ref()
            r = Map.put(state.refs, ref, path)
            d = Map.put(state.devices, path, ref)
            new_state = %{devices: d, refs: r}
            {:reply, {:ok, ref}, new_state}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end

      dev ->
        {:reply, {:ok, dev}, state}
    end
  end

  def handle_call({:get_path, ref}, _from, state) do
    case Map.get(state.refs, ref) do
      nil ->
        {:reply, {:error, :device_not_found}, state}

      path ->
        {:reply, {:ok, path}, state}
    end
  end

  def handle_call({:get_client, ref}, _from, %{control_points: controls} = s) do
    client = Map.get(controls, ref)

    if client != nil do
      {:reply, {:ok, client}, s}
    else
      {:reply, {:error, "control_point not found"}, s}
    end
  end

  def handle_info(msg, s) do
    Logger.warning("unexpected message in Eqmi: #{inspect(msg)}")
    {:noreply, s}
  end

  def terminate(reason, _s) do
    Logger.debug("Eqmi terminating: #{inspect(reason)}")
  end
end
