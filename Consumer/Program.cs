using System;
using Confluent.Kafka;

namespace HealthCareManagement.Consumer
{
    class Consumer
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                
                //Kafka broker adresi ve portu belirtilir.
                BootstrapServers = "localhost:9092",
                
                //Tüketici grubun kimliği belirtilir.
                GroupId = "hastalar",
                
                //Tüketici grubunun kaydedilmiş bir offseti yoksa veya offset mevcut değilse tüketici en eski mesajdan başlar.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            //Tüketici nesnesi oluşturulur ve yapılandırılır.
            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                
                //Tüketici, ilgili topic'e abone olur, bu sayede gelen mesajlar dinlenir.
                consumer.Subscribe("healthcare-topic");

                //Kafkadan sürekli olarak mesaj alınır.
                while (true)
                {
                    //Tüketici bir mesajı alır.
                    var consumeResult = consumer.Consume();

                    //Tüketilen mesaj değeri konsola yazdırılır.
                    Console.WriteLine($"Alınan mesaj: {consumeResult.Message.Value}");
                }
            }
        }
    }
}