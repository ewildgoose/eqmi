defmodule Eqmi.Reader do
  use Task
  require Logger

  # Largest control message we ever expect; qmicli reports
  # wMaxControlMessage = 4096 for this class of device.
  @max_len 4096

  def start_link(pid, dev) do
    Task.start_link(__MODULE__, :run, [pid, dev])
  end

  def run(pid, dev) do
    options = [:read, :raw]
    {:ok, fd} = File.open(dev, options)
    run_priv(pid, fd)
  end

  # Reads on a raw fd block until the full requested size has been
  # accumulated (the file layer retries short reads), so only ever
  # request bytes we know are coming: the 3-byte frame header, then
  # exactly the length it announces.
  defp run_priv(pid, dev) do
    with {:ok, len} <- read_header(dev),
         body when is_binary(body) and byte_size(body) == len - 2 <-
           IO.binread(dev, len - 2) do
      <<h::binary-size(3), sdu::binary>> = body
      header = Eqmi.QmuxHeader.parse(h)
      send(pid, {:qmux, header, sdu})
      run_priv(pid, dev)
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
end
