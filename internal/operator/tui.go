package operator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"gitlab.com/gitlab-org/fleeting/plugins/proxmox/internal/metrics"
)

type tuiResult struct {
	status  metrics.ControlStatus
	err     error
	command string
}

// tview.SetScreen discards Init errors, so retain the error for the caller.
type checkedScreen struct {
	tcell.Screen
	err error
}

func (s *checkedScreen) Init() error { s.err = s.Screen.Init(); return s.err }

func RunTUI(ctx context.Context, opts Options) error {
	screen, err := tcell.NewScreen()
	if err != nil {
		return err
	}
	return runTUI(ctx, opts, screen)
}

func runTUI(parent context.Context, opts Options, screen tcell.Screen) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	checked := &checkedScreen{Screen: screen}
	app := tview.NewApplication().SetScreen(checked)
	if checked.err != nil {
		screen.Fini()
		return checked.err
	}
	nodes := tview.NewTable().SetFixed(1, 0).SetSelectable(true, false)
	nodes.SetBorder(true).SetTitle(" Local nodes ")
	detail := tview.NewTextView().SetScrollable(true).SetWrap(false)
	detail.SetBorder(true)
	message := tview.NewTextView().SetText("Connecting...")
	help := tview.NewTextView().SetText("Space select  d drain  u undrain  Tab focus  1 resources  2 storages  3 VMs  4 problems  5 fleets  q quit")
	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(message, 2, 0, false).AddItem(nodes, 0, 1, true).AddItem(detail, 0, 2, false).AddItem(help, 2, 0, false)
	app.SetRoot(layout, true).SetFocus(nodes)
	results := make(chan tuiResult, 4)
	var workers sync.WaitGroup
	sendResult := func(result tuiResult) {
		select {
		case results <- result:
			_ = screen.PostEvent(tcell.NewEventKey(tcell.KeyRune, 0, tcell.ModNone))
		case <-ctx.Done():
		}
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			requestCtx, stop := context.WithTimeout(ctx, 5*time.Second)
			status, err := metrics.RequestControl(requestCtx, opts.Socket, metrics.ControlRequest{Action: "status"})
			stop()
			sendResult(tuiResult{status: status, err: err})
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	var status metrics.ControlStatus
	var rows []metrics.DrainStatus
	selected := map[string]metrics.NodeRef{}
	view := "nodes"
	busy := false
	commandMessage := ""
	renderDetail := func() {
		filter := opts
		row, _ := nodes.GetSelection()
		if view == "problems" {
			filter = Options{}
		} else if row > 0 && row <= len(rows) {
			filter.Cluster = rows[row-1].Cluster
			filter.Node = rows[row-1].Node
		}
		var out bytes.Buffer
		Render(&out, view, status, filter)
		detail.SetTitle(" " + view + " ")
		if view == "problems" {
			detail.SetTitle(" problems (all local fleets) ")
		}
		detail.SetText(strings.ReplaceAll(out.String(), "\t", "  "))
	}
	render := func() {
		row, _ := nodes.GetSelection()
		current := ""
		if row > 0 && row <= len(rows) {
			current = rows[row-1].Key()
		}
		nodes.Clear()
		rows = nil
		for col, label := range []string{"", "Cluster", "Node", "Placement", "Applied", "Empty", "VMs", "Pending", "Templates"} {
			nodes.SetCell(0, col, tview.NewTableCell(label).SetSelectable(false).SetTextColor(tcell.ColorYellow))
		}
		for _, node := range status.Nodes {
			if !matches(opts, node.Cluster, node.Node) {
				continue
			}
			rows = append(rows, node)
			mark := " "
			if _, ok := selected[node.Key()]; ok {
				mark = "*"
			}
			placement := "enabled"
			if node.Drained {
				placement = "drained"
			}
			values := []string{mark, node.Cluster, node.Node, placement, fmt.Sprint(node.Applied), fmt.Sprint(node.Empty), fmt.Sprint(node.Instances), fmt.Sprint(node.Pending), fmt.Sprint(node.Templates)}
			for col, value := range values {
				nodes.SetCell(len(rows), col, tview.NewTableCell(tview.Escape(safe(value))))
			}
			if current == node.Key() {
				row = len(rows)
			}
		}
		if len(rows) > 0 {
			if row < 1 {
				row = 1
			}
			if row > len(rows) {
				row = len(rows)
			}
			nodes.Select(row, 0)
		}
		message.SetText(fmt.Sprintf("%s | errors %d | recent errors %d | fleets %d | revision %d | selected %d\n%s", status.Problems.Status, status.Problems.Active, status.Problems.Recent, len(status.Fleets), status.State.Revision, len(selected), safe(commandMessage)))
		renderDetail()
	}
	nodes.SetSelectionChangedFunc(func(int, int) { renderDetail() })
	app.SetBeforeDrawFunc(func(tcell.Screen) bool {
		for {
			select {
			case result := <-results:
				if result.command != "" {
					busy = false
				}
				if result.err != nil {
					commandMessage = result.err.Error()
					message.SetText("Connection/command error: " + safe(commandMessage))
				} else {
					status = result.status
					if result.command != "" {
						commandMessage = result.command + " saved; check Applied and Empty columns"
					}
					render()
				}
			default:
				return false
			}
		}
	})
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyCtrlC || event.Rune() == 'q' {
			cancel()
			app.Stop()
			return nil
		}
		if event.Key() == tcell.KeyTab {
			if nodes.HasFocus() {
				app.SetFocus(detail)
			} else {
				app.SetFocus(nodes)
			}
			return nil
		}
		if event.Rune() == 0 && event.Key() == tcell.KeyRune {
			return nil
		}
		if event.Rune() >= '1' && event.Rune() <= '5' {
			view = []string{"nodes", "storages", "instances", "problems", "fleets"}[event.Rune()-'1']
			detail.ScrollToBeginning()
			renderDetail()
			return nil
		}
		if event.Rune() == ' ' && nodes.HasFocus() {
			row, _ := nodes.GetSelection()
			if row > 0 && row <= len(rows) {
				ref := rows[row-1].NodeRef
				if _, ok := selected[ref.Key()]; ok {
					delete(selected, ref.Key())
				} else {
					selected[ref.Key()] = ref
				}
				render()
			}
			return nil
		}
		if event.Rune() == 'd' || event.Rune() == 'u' {
			if busy {
				return nil
			}
			var refs []metrics.NodeRef
			for _, ref := range selected {
				refs = append(refs, ref)
			}
			if len(refs) == 0 {
				row, _ := nodes.GetSelection()
				if row > 0 && row <= len(rows) {
					refs = append(refs, rows[row-1].NodeRef)
				}
			}
			if len(refs) == 0 {
				return nil
			}
			action := "drain"
			if event.Rune() == 'u' {
				action = "undrain"
			}
			busy = true
			commandMessage = action + " in progress"
			render()
			workers.Add(1)
			go func() {
				defer workers.Done()
				commandCtx, stop := context.WithTimeout(ctx, opts.Timeout)
				defer stop()
				result, err := metrics.RequestControl(commandCtx, opts.Socket, metrics.ControlRequest{Action: action, Nodes: refs})
				sendResult(tuiResult{status: result, err: err, command: action})
			}()
			return nil
		}
		return event
	})
	stop := context.AfterFunc(ctx, func() { _ = screen.PostEvent(tcell.NewEventKey(tcell.KeyCtrlC, 0, tcell.ModNone)) })
	err := app.Run()
	stop()
	cancel()
	workers.Wait()
	return err
}
