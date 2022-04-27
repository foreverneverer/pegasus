package executor

import (
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"os"
	"strings"
	"time"
)

func CopyData(client *Client, from string, target string, timeStart string, timeEnd string, copy bool) error {

	tables, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
	if err != nil {
		return err
	}

	metaAddrsFrom := strings.Split(from, ",")
	metaAddrsTarget := strings.Split(target, ",")
	targetAdminClient := NewClient(os.Stdout, metaAddrsTarget)
	for _, tb := range tables {
		if copy {
			fmt.Println("表已经建好")
			break
		}
		if  util.FormatDate(tb.CreateSecond) < timeEnd && util.FormatDate(tb.CreateSecond) > timeStart  {
			_, err := targetAdminClient.Meta.CreateApp(tb.AppName, map[string]string{}, 4)
			fmt.Println("create ", tb.AppName, util.FormatDate(tb.CreateSecond), err.Error())
			if strings.Contains(err.Error(), "EXIST") {
				fmt.Println(err.Error(), "忽略")
				time.Sleep(time.Microsecond * 100)
				continue
			}
			time.Sleep(time.Second * 1)
		}
	}

	time.Sleep(time.Second * 20)
	fmt.Println("开始迁移")
	fromCfg := pegasus.Config{
		MetaServers: metaAddrsFrom,
	}
	targetCfg := pegasus.Config{
		MetaServers: metaAddrsTarget,
	}

	fromUserClient := pegasus.NewClient(fromCfg)
	targetUserClient := pegasus.NewClient(targetCfg)
	ctx := context.Background()
	for _, tb := range tables {
		if  strings.Contains(tb.AppName, "loki") && util.FormatDate(tb.CreateSecond) < timeEnd && util.FormatDate(tb.CreateSecond) > timeStart {
			fmt.Println("开始迁移", tb.AppName)
			var tableConnectorFrom pegasus.TableConnector
			var tableConnectorTarget pegasus.TableConnector
			for {
				tableConnectorFrom, err = fromUserClient.OpenTable(ctx, tb.AppName)
				if err != nil {
					fmt.Println(err.Error())
					time.Sleep(time.Second * 10)
					continue
				}
				break
			}

			for {
				tableConnectorTarget, err = targetUserClient.OpenTable(ctx, tb.AppName)
				if err != nil {
					fmt.Println(err.Error())
					time.Sleep(time.Second * 10)
					continue
				}
				break
			}

			var ok bool
			var count int32
			for {
				scanner, err := tableConnectorFrom.GetUnorderedScanners(ctx, 1, &pegasus.ScannerOptions{})
				if err != nil {
					fmt.Println(err.Error())
					time.Sleep(time.Second * 10)
					continue
				}

				for {
					complete, hash, sort, value, err := scanner[0].Next(ctx)
					if err != nil {
						fmt.Println(err.Error())
						time.Sleep(time.Second * 10)
						continue
					}
					if complete {
						ok = true
						break
					}

					for {
						err := tableConnectorTarget.Set(ctx, hash, sort, value)
						if err != nil {
							fmt.Println(err.Error())
							time.Sleep(time.Second * 10)
							continue
						}

						count = count + 1
						if count % 1000 == 0 {
							fmt.Println(tb.AppName, count)
						}
						break
					}

				}

				if ok {
					fmt.Println(tb.AppName, "ok")
					break
				}
			}

		}
	}

	return err
}