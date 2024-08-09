package pkg

import (
	"fmt"
	"os"
	"testing"
)

func TestAES(t *testing.T) {
	key, _ := os.ReadFile("aes.key")

	a := NewAes(key, iv)
	encData, _ := os.ReadFile("enc.data")
	decData := make([]byte, len(encData))
	decData = a.Dec(encData)
	fmt.Println(string(decData))

	encData = a.Enc([]byte("Hello from golang"))
	os.WriteFile("enc.data", encData, 0644)
}
