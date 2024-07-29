using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

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
                GroupId = "consumerGroup",
                
                //Tüketici yeni mesajlardan itibaren okumaya başlar.
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false 
            };

            try
            {
                using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092" }).Build())
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    while (true)
                    {
                        try
                        {
                            List<string> topics = new List<string>();
                            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                            
                            // Mevcut topicler listelenir.
                            foreach (var topic in metadata.Topics)
                            {
                                if (!topic.Error.IsError)
                                {
                                    topics.Add(topic.Topic);
                                }
                            }

                            Console.WriteLine("Topicler:");
                            for (int i = 0; i < topics.Count; i++)
                            {
                                Console.WriteLine($"{i + 1}. {topics[i]}");
                            }

                            Console.Write("Dinlemek istediğiniz topici seçin: ");
                            var input = Console.ReadLine();
                            int topicIndex;
                            if (int.TryParse(input, out topicIndex) && topicIndex > 0 && topicIndex <= topics.Count)
                            {
                                string topicName = topics[topicIndex - 1];
                                consumer.Subscribe(topicName);

                                Console.WriteLine($"'{topicName}' topic'inden mesajlar dinleniyor (Çıkmak için 'çıkış' yazın, yeni topic seçmek için 'yeni' yazın):");

                                while (true)
                                {
                                    try
                                    {
                                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                                        if (consumeResult != null)
                                        {
                                            Console.WriteLine($"Alınan mesaj: {consumeResult.Message.Value}");

                                            // Mesajı işlendikten sonra commit edilir
                                            consumer.Commit(consumeResult);
                                        }

                                        // Kullanıcıdan bir komut girmesi istenir
                                        if (Console.KeyAvailable)
                                        {
                                            Console.Write("Komut girin (Çıkmak için 'çıkış', yeni topic için 'yeni'): ");
                                            var command = Console.ReadLine();
                                            
                                            if (command.ToLower() == "çıkış")
                                            {
                                                return;
                                            }
                                            else if (command.ToLower() == "yeni")
                                            {
                                                consumer.Unsubscribe();
                                                break;
                                            }
                                        }
                                    }
                                    catch (ConsumeException ex)
                                    {
                                        Console.WriteLine($"Mesaj tüketim hatası: {ex.Error.Reason}");
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Bir hata oluştu: {ex.Message}");
                                    }
                                }
                            }
                            else
                            {
                                Console.WriteLine("Geçersiz seçim, tekrar deneyin.");
                            }
                        }
                        catch (KafkaException ex)
                        {
                            Console.WriteLine($"Kafka ile ilgili bir hata oluştu: {ex.Message}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Bir hata oluştu: {ex.Message}");
                        }
                    }
                }
            }
            catch (KafkaException ex)
            {
                Console.WriteLine($"Kafka ile ilgili bir hata oluştu: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Bir hata oluştu: {ex.Message}");
            }
        }
    }
}
