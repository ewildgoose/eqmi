defmodule Eqmi.Mbim do
  @moduledoc """
  Minimal MBIM framing for carrying QMI over an MBIM `cdc-wdm` device
  (QMI-over-MBIM passthrough).

  Only what QMI needs: the OPEN handshake and the QMI device-service command
  that wraps a raw QMI message. The QMI message is carried verbatim in the
  MBIM information buffer, including its leading `0x01` QMUX marker, so the
  rest of eqmi is unchanged - MBIM is pure envelope. See libqmi's
  `qmi-endpoint-mbim.c`.
  """

  # MBIM QMI device service UUID (stored as written, network order) + CID.
  @uuid_qmi <<0xD1, 0xA3, 0x0B, 0xC2, 0xF9, 0x7A, 0x6E, 0x43, 0xBF, 0x65, 0xC7, 0xE2, 0x4F, 0xB0,
              0xF0, 0xD3>>
  @cid_qmi_msg 1
  @command_type_set 1

  # MBIM message types
  @open 0x00000001
  @close 0x00000002
  @command 0x00000003
  @open_done 0x80000001
  @close_done 0x80000002
  @command_done 0x80000003
  @function_error 0x80000004
  @indicate_status 0x80000007

  @default_max_control_transfer 4096

  @doc "MBIM_OPEN_MSG establishing the session and max control transfer size."
  @spec open(non_neg_integer(), pos_integer()) :: binary()
  def open(tx_id, max_transfer \\ @default_max_control_transfer) do
    body = <<max_transfer::little-unsigned-32>>
    header(@open, tx_id, body)
  end

  @doc "MBIM_CLOSE_MSG tearing down the session (no body)."
  @spec close(non_neg_integer()) :: binary()
  def close(tx_id), do: header(@close, tx_id, <<>>)

  @doc """
  Wrap a raw QMI message (with its `0x01` marker) in an MBIM_COMMAND_MSG for
  the QMI service. Single fragment - QMI control messages fit in one.

  The information buffer is *not* padded: the device requires
  `MessageLength == 48 + InformationBufferLength`, and padding counted into
  MessageLength is rejected with MBIM error 3 (LENGTH_MISMATCH).
  """
  @spec command(non_neg_integer(), binary()) :: binary()
  def command(tx_id, qmi_message) do
    body =
      <<1::little-unsigned-32, 0::little-unsigned-32>> <>
        @uuid_qmi <>
        <<@cid_qmi_msg::little-unsigned-32>> <>
        <<@command_type_set::little-unsigned-32>> <>
        <<byte_size(qmi_message)::little-unsigned-32>> <>
        qmi_message

    header(@command, tx_id, body)
  end

  @doc "The full MBIM message length (header + body); used to frame reads."
  @spec message_length(binary()) :: non_neg_integer()
  def message_length(<<_type::little-32, len::little-unsigned-32, _rest::binary>>), do: len

  @doc """
  Decode one complete MBIM message. Returns:

    * `{:open_done, status}`   - OPEN handshake result (0 = success)
    * `{:close_done, status}`  - CLOSE result
    * `{:qmi, qmi_message}`    - a QMI response/indication (with `0x01` marker)
    * `{:fragmented, total}`   - multi-fragment message (unsupported)
    * `{:function_error, err}` - device-level MBIM error
    * `{:other, type}`         - anything else, ignored by the caller
  """
  @spec parse(binary()) ::
          {:open_done, non_neg_integer()}
          | {:close_done, non_neg_integer()}
          | {:qmi, binary()}
          | {:fragmented, pos_integer()}
          | {:function_error, non_neg_integer()}
          | {:other, non_neg_integer()}
  def parse(<<@open_done::little-32, _len::little-32, _tx::little-32, status::little-unsigned-32, _::binary>>) do
    {:open_done, status}
  end

  def parse(<<@close_done::little-32, _len::little-32, _tx::little-32, status::little-unsigned-32, _::binary>>) do
    {:close_done, status}
  end

  def parse(<<@command_done::little-32, _len::little-32, _tx::little-32, body::binary>>) do
    # fragment(8) + uuid(16) + cid(4) + status(4) + info_len(4) + info
    <<total::little-unsigned-32, _current::little-32, uuid::binary-size(16), _cid::little-32,
      _status::little-32, info_len::little-unsigned-32, info::binary>> = body

    qmi_from(total, uuid, info, info_len)
  end

  def parse(<<@indicate_status::little-32, _len::little-32, _tx::little-32, body::binary>>) do
    # like command_done but with no status field
    <<total::little-unsigned-32, _current::little-32, uuid::binary-size(16), _cid::little-32,
      info_len::little-unsigned-32, info::binary>> = body

    qmi_from(total, uuid, info, info_len)
  end

  def parse(<<@function_error::little-32, _len::little-32, _tx::little-32, status::little-unsigned-32, _::binary>>) do
    {:function_error, status}
  end

  def parse(<<type::little-32, _::binary>>) do
    {:other, type}
  end

  # Only single-fragment messages are supported; QMI control messages fit
  # well inside the 4096-byte max control transfer. Anything larger would
  # need reassembly, so surface it rather than mis-parse a partial buffer.
  defp qmi_from(total, _uuid, _info, _len) when total > 1, do: {:fragmented, total}

  defp qmi_from(_total, @uuid_qmi, info, info_len) do
    <<qmi::binary-size(info_len), _pad::binary>> = info
    {:qmi, qmi}
  end

  defp qmi_from(_total, _other_uuid, _info, _len), do: {:other, @indicate_status}

  defp header(type, tx_id, body) do
    <<type::little-unsigned-32, 12 + byte_size(body)::little-unsigned-32, tx_id::little-unsigned-32>> <>
      body
  end
end
