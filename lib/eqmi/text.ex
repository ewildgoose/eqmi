defmodule Eqmi.Text do
  @moduledoc """
  Charset handling for QMI strings.

  Modems return string TLVs in a mix of encodings (operator names
  especially): plain ASCII/UTF-8, GSM 7-bit packed (3GPP TS 23.038) or
  UCS-2LE. Mirrors libqmi's qmi_message_tlv_read_string fallback chain:
  printable UTF-8 is used as is, otherwise try GSM-7, then UCS-2LE, and
  keep the raw bytes if nothing matches.
  """
  import Bitwise

  # GSM 7-bit default alphabet (3GPP TS 23.038), septet -> codepoint.
  # 0x1B is the escape to the extension table.
  @gsm_basic {?@, ?£, ?$, ?¥, ?è, ?é, ?ù, ?ì,
              ?ò, ?Ç, ?\n, ?Ø, ?ø, ?\r, ?Å, ?å,
              ?Δ, ?_, ?Φ, ?Γ, ?Λ, ?Ω, ?Π, ?Ψ,
              ?Σ, ?Θ, ?Ξ, :esc, ?Æ, ?æ, ?ß, ?É,
              ?\s, ?!, ?", ?#, ?¤, ?%, ?&, ?',
              ?(, ?), ?*, ?+, ?,, ?-, ?., ?/,
              ?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7,
              ?8, ?9, ?:, ?;, ?<, ?=, ?>, ??,
              ?¡, ?A, ?B, ?C, ?D, ?E, ?F, ?G,
              ?H, ?I, ?J, ?K, ?L, ?M, ?N, ?O,
              ?P, ?Q, ?R, ?S, ?T, ?U, ?V, ?W,
              ?X, ?Y, ?Z, ?Ä, ?Ö, ?Ñ, ?Ü, ?§,
              ?¿, ?a, ?b, ?c, ?d, ?e, ?f, ?g,
              ?h, ?i, ?j, ?k, ?l, ?m, ?n, ?o,
              ?p, ?q, ?r, ?s, ?t, ?u, ?v, ?w,
              ?x, ?y, ?z, ?ä, ?ö, ?ñ, ?ü, ?à}

  # extension table (after 0x1B escape); anything else is invalid
  @gsm_ext %{
    0x0A => ?\f,
    0x14 => ?^,
    0x28 => ?{,
    0x29 => ?},
    0x2F => ?\\,
    0x3C => ?[,
    0x3D => ?~,
    0x3E => ?],
    0x40 => ?|,
    0x65 => ?€
  }

  @doc """
  Decode raw QMI string bytes into a printable UTF-8 string.
  """
  def decode_string(bin) when is_binary(bin) do
    stripped = trim_trailing_nulls(bin)

    cond do
      String.printable?(stripped) -> stripped
      (s = from_gsm7(stripped)) != nil -> s
      (s = from_ucs2le(stripped)) != nil -> s
      true -> bin
    end
  end

  @doc """
  Unpack a GSM 7-bit packed binary into a UTF-8 string, or nil if any
  septet has no mapping.
  """
  def from_gsm7(bin) do
    # unpack_septets accumulates in reverse, which puts the trailing
    # padding zeros conveniently at the head
    bin
    |> unpack_septets(0, 0, [])
    |> trim_leading_zeros()
    |> Enum.reverse()
    |> map_gsm([])
  end

  @doc """
  Convert UCS-2 little-endian bytes to UTF-8, or nil if not valid.
  """
  def from_ucs2le(bin) when rem(byte_size(bin), 2) == 0 and bin != <<>> do
    case :unicode.characters_to_binary(bin, {:utf16, :little}, :utf8) do
      s when is_binary(s) -> if String.printable?(s), do: s, else: nil
      _ -> nil
    end
  end

  def from_ucs2le(_bin), do: nil

  defp trim_trailing_nulls(bin) do
    String.trim_trailing(bin, <<0>>)
  end

  # accumulate bits LSB first; every 7 bits is one septet, trailing
  # spare bits are padding
  defp unpack_septets(<<>>, _buffer, _nbits, acc), do: acc

  defp unpack_septets(<<octet, rest::binary>>, buffer, nbits, acc) do
    {buffer, nbits, acc} = drain(buffer ||| octet <<< nbits, nbits + 8, acc)
    unpack_septets(rest, buffer, nbits, acc)
  end

  defp drain(buffer, nbits, acc) when nbits >= 7 do
    drain(buffer >>> 7, nbits - 7, [buffer &&& 0x7F | acc])
  end

  defp drain(buffer, nbits, acc), do: {buffer, nbits, acc}

  # 0x00 septets at the end of the message are padding, not '@'
  defp trim_leading_zeros([0 | rest]), do: trim_leading_zeros(rest)
  defp trim_leading_zeros(septets), do: septets

  defp map_gsm([], acc) do
    acc |> Enum.reverse() |> List.to_string()
  end

  defp map_gsm([0x1B, ext | rest], acc) do
    case Map.get(@gsm_ext, ext) do
      nil -> nil
      c -> map_gsm(rest, [c | acc])
    end
  end

  # dangling escape at the end: not a valid GSM-7 string
  defp map_gsm([0x1B], _acc), do: nil

  defp map_gsm([septet | rest], acc) do
    map_gsm(rest, [elem(@gsm_basic, septet) | acc])
  end
end
