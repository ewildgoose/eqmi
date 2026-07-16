defmodule Eqmi.Reader do
  use Task
  require Logger

  # Largest control message we ever expect; qmicli reports
  # wMaxControlMessage = 4096 for this class of device.
  @max_len 4096

  def start_link(pid, dev, transport \\ :raw) do
    Task.start_link(__MODULE__, :run, [pid, dev, transport])
  end

  def run(pid, dev, transport \\ :raw) do
    options = [:read, :raw]
    {:ok, fd} = File.open(dev, options)

    case transport do
      :raw -> raw_loop(pid, fd)
      :mbim -> mbim_loop(pid, fd)
    end
  end

  # --- raw QMUX transport -------------------------------------------------
  #
  # Reads on a raw fd block until the full requested size has been
  # accumulated (the file layer retries short reads), so only ever request
  # bytes we know are coming: the 3-byte frame header, then exactly the
  # length it announces.
  defp raw_loop(pid, dev) do
    with {:ok, len} <- read_header(dev),
         body when is_binary(body) and byte_size(body) == len - 2 <-
           IO.binread(dev, len - 2) do
      emit_qmux(pid, <<1, len::little-unsigned-integer-size(16)>> <> body)
      raw_loop(pid, dev)
    else
      {:error, reason} -> send(pid, {:error, reason})
      _eof_or_short -> send(pid, {:error, :eof})
    end
  end

  # A frame starts with if_type 0x01 followed by a little-endian u16
  # length that counts every byte after the if_type: 3 QMUX header bytes
  # plus the SDU.
  defp read_header(dev) do
    case IO.binread(dev, 3) do
      <<1, len::little-unsigned-integer-size(16)>> when len >= 5 and len <= @max_len ->
        {:ok, len}

      <<_, _, _>> = junk ->
        resync(dev, junk, 0)

      other ->
        other
    end
  end

  # Stale data, e.g. the tail of a frame partially consumed by a previous
  # reader still sitting in the cdc-wdm buffer: scan byte-wise until a
  # plausible frame header comes round again.
  defp resync(_dev, <<1, len::little-unsigned-integer-size(16)>>, dropped)
       when len >= 5 and len <= @max_len do
    Logger.warning("qmux resync: dropped #{dropped} bytes of stale data")
    {:ok, len}
  end

  defp resync(dev, <<_junk, keep::binary-size(2)>>, dropped) do
    case IO.binread(dev, 1) do
      <<b>> -> resync(dev, <<keep::binary, b>>, dropped + 1)
      other -> other
    end
  end

  # --- QMI-over-MBIM transport --------------------------------------------
  #
  # Read one MBIM message at a time (12-byte header carries the total
  # length), decode it, and for QMI messages hand the embedded QMUX frame to
  # the same emit path as the raw transport.
  defp mbim_loop(pid, dev) do
    case read_mbim(dev) do
      {:ok, msg} ->
        dispatch_mbim(pid, Eqmi.Mbim.parse(msg))
        mbim_loop(pid, dev)

      {:error, reason} ->
        send(pid, {:error, reason})
    end
  end

  defp read_mbim(dev) do
    with <<_::binary-size(12)>> = header <- IO.binread(dev, 12),
         len = Eqmi.Mbim.message_length(header),
         body when is_binary(body) and byte_size(body) == len - 12 <-
           IO.binread(dev, len - 12) do
      {:ok, header <> body}
    else
      {:error, reason} -> {:error, reason}
      _eof_or_short -> {:error, :eof}
    end
  end

  defp dispatch_mbim(pid, {:open_done, status}), do: send(pid, {:mbim_open_done, status})
  defp dispatch_mbim(pid, {:close_done, status}), do: send(pid, {:mbim_close_done, status})
  defp dispatch_mbim(pid, {:qmi, frame}), do: emit_qmux(pid, frame)

  defp dispatch_mbim(_pid, {:function_error, status}),
    do: Logger.error("MBIM function error status #{status}")

  defp dispatch_mbim(_pid, {:fragmented, total}),
    do: Logger.error("dropping fragmented MBIM response (#{total} fragments); reassembly unimplemented")

  defp dispatch_mbim(_pid, {:other, type}),
    do: Logger.debug("ignoring MBIM message type #{inspect(type, base: :hex)}")

  # --- shared -------------------------------------------------------------
  #
  # A complete QMUX frame (0x01 marker, len, 3-byte header, SDU) - from
  # either transport - becomes a {:qmux, header, sdu} message.
  defp emit_qmux(pid, <<1, len::little-unsigned-integer-size(16), rest::binary>>)
       when len >= 5 do
    body_len = len - 2
    <<body::binary-size(body_len), _extra::binary>> = rest
    <<h::binary-size(3), sdu::binary>> = body
    send(pid, {:qmux, Eqmi.QmuxHeader.parse(h), sdu})
  end
end
