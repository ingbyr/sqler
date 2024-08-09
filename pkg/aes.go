package pkg

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
)

var DefaultIV = []byte("21a34b56c78d90ef")

type AesCipher struct {
	key []byte
	iv  []byte
	enc cipher.Stream
	dec cipher.Stream
}

func NewAes(key []byte, iv []byte) *AesCipher {
	if len(iv) == 0 {
		iv = DefaultIV
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	a := &AesCipher{
		key: key,
		iv:  iv,
		enc: cipher.NewCFBEncrypter(block, iv),
		dec: cipher.NewCFBDecrypter(block, iv),
	}
	return a
}

func (c *AesCipher) Enc(data []byte) []byte {
	b := make([]byte, len(data))
	c.enc.XORKeyStream(b, data)
	return b
}

func (c *AesCipher) EncAsHex(data string) string {
	return hex.EncodeToString(c.Enc([]byte(data)))
}

func (c *AesCipher) Dec(data []byte) []byte {
	b := make([]byte, len(data))
	c.dec.XORKeyStream(b, data)
	return b
}

func (c *AesCipher) DecAsStr(hexData string) string {
	data, err := hex.DecodeString(hexData)
	if err != nil {
		panic(err)
	}
	return string(c.Dec(data))
}
