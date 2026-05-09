const patch = snabbdom.init([
  snabbdom.attributesModule,
  snabbdom.eventListenersModule,
  snabbdom.propsModule,
  snabbdom.styleModule,
])

const h = snabbdom.h

let container = document.getElementById("app")

// --- State ---
let filterStatus = ""
let filterFunction = ""
let wsConnected = false

const render = function(vnode) {
  patch(container, vnode)
  container = vnode
}

const renderPage = _ => page().then(view => render(view))

window.addEventListener("popstate", event => renderPage())

const handle_error = function(error) {
  console.log(error)
  return { error: error.toString() }
}

const apiPath = function(path) {
  return root_path + "/api" + path.replace(RegExp(`^${root_path}`), '')
}

const get = async function(path) {
  try {
    const response = await fetch(apiPath(path))
    return await response.json()
  } catch (error) {
    return handle_error(error)
  }
}

const post = async function(path, data) {
  try {
    const response = await fetch(
      apiPath(path),
      {
        method: "post",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      },
    )
    return await response.json()
  } catch (error) {
    return handle_error(error)
  }
}

const button = function(children, handler, data) {
  data.attrs ||= {}
  data.attrs.role = "button"
  data.on ||= {}
  data.on.click = async event => {
    event.target.setAttribute("aria-busy", true)
    await handler(event)
    event.target.setAttribute("aria-busy", false)
    renderPage()
  }

  return h("a", data, children)
}

const sm_button = function(label, handler, danger) {
  return h("a", {
    attrs: { role: "button" },
    class: { "btn-danger": !!danger },
    on: { click: async event => {
      event.preventDefault()
      event.stopPropagation()
      event.target.setAttribute("aria-busy", true)
      await handler()
      event.target.setAttribute("aria-busy", false)
      renderPage()
    }},
  }, label)
}

const link = function(data, children) {
  const handler = function(event) {
    event.preventDefault()
    const path = data.props.href
    page(path).then(view => render(view))
    window.history.pushState(null, null, path)
    event.target.blur()
  }

  return h("a", Object.assign({ on: { click: handler } }, data), children)
}

const format_time = time => time ? new Date(time).toLocaleString() : ""

const home_view = function(data) {
  return h("div", [
    h("h1", "Queues"),
    h("table", [
      h("thead", [
        h("tr", [
          h("th", "Queue"),
          h("th", "Active"),
          h("th", "Queued"),
          h("th", "Scheduled"),
          h("th", "Workers"),
        ]),
      ]),
      h("tbody", { attrs: { role: "grid" } }, data.queues.map(queue =>
        h("tr", [
          h("td", link({ props: { href: root_path + "/queues/" + queue.name } }, queue.name)),
          h("td", queue.active),
          h("td", queue.queued),
          h("td", queue.scheduled),
          h("td", Object.keys(queue["workers"]).length),
        ])
      )),
    ]),
  ])
}

const job_headers = () => [
  h("th", "Function"),
  h("th", "Args"),
  h("th", "Queued"),
  h("th", "Started"),
  h("td", "Completed"),
  h("th", "Status"),
  h("th", "Actions"),
]

const job_action_buttons = function(queue_name, job) {
  const status = job.status
  const btns = []
  if (status === "failed" || status === "aborted" || status === "aborting" || status === "complete") {
    btns.push(sm_button("Retry", _ =>
      post(root_path + "/queues/" + queue_name + "/jobs/" + job.key + "/retry")
    ))
  }
  if (status === "queued" || status === "active" || status === "new" || status === "aborting") {
    btns.push(sm_button("Abort", _ =>
      post(root_path + "/queues/" + queue_name + "/jobs/" + job.key + "/abort")
    , true))
  }
  return h("td", h("span", { class: { "action-btns": true } }, btns))
}

const job_columns = (queue_name, job) => [
  h("td", job.function),
  h("td", job.kwargs),
  h("td", format_time(job.queued)),
  h("td", format_time(job.started)),
  h("td", format_time(job.completed)),
  h("td", job.status),
  job_action_buttons(queue_name, job),
]

const status_options = [
  { value: "", label: "Filter by status..." },
  { value: "new", label: "New" },
  { value: "queued", label: "Queued" },
  { value: "active", label: "Active" },
  { value: "aborting", label: "Aborting" },
  { value: "aborted", label: "Aborted" },
  { value: "failed", label: "Failed" },
  { value: "complete", label: "Complete" },
]

const fetchFilteredJobs = async function(queue_name) {
  const params = new URLSearchParams()
  if (filterStatus) params.set("status", filterStatus)
  if (filterFunction) params.set("function", filterFunction)
  const qs = params.toString()
  const path = root_path + "/queues/" + queue_name + "/jobs" + (qs ? "?" + qs : "")
  const data = await get(path)
  return data.jobs || []
}

const queue_view = function(data, queue_name, filteredJobs) {
  const queue = data.queue
  const jobs = filteredJobs || queue.jobs || []

  return h("div", [
    h("hgroup", [
      h("h1", "Queue"),
      h("h2", queue_name),
    ]),
    h("table", [
      h("thead", [
        h("tr", [
          h("th", "Active"),
          h("th", "Queued"),
          h("th", "Scheduled"),
        ]),
      ]),
      h("tbody", h("tr", [
        h("td", queue.active),
        h("td", queue.queued),
        h("td", queue.scheduled),
      ])),
    ]),
    h("h2", "Workers"),
    h("table", { attrs: { role: "grid" } }, [
      h("thead", [
        h("tr", [
          h("th", "Worker"),
          h("th", "Complete"),
          h("th", "Retried"),
          h("th", "Failed"),
          h("th", "Uptime (s)"),
        ]),
      ]),
      h("tbody", Object.entries(queue.workers).map(([name, worker]) =>
        h("tr", [
          h("td", name),
          h("td", worker.stats.complete),
          h("td", worker.stats.retried),
          h("td", worker.stats.failed),
          h("td", worker.stats.uptime / 1000),
        ])
      )),
    ]),
    h("h2", "Jobs"),
    // Filter bar
    h("div", { style: { display: "flex", gap: "1rem", marginBottom: "1rem", alignItems: "center" } }, [
      h("label", [
        "Status ",
        h("select", {
          props: { value: filterStatus },
          on: { change: event => { filterStatus = event.target.value; renderPage() } },
        }, status_options.map(opt =>
          h("option", { props: { value: opt.value } }, opt.label)
        )),
      ]),
      h("label", [
        "Function ",
        h("input", {
          props: { type: "text", value: filterFunction, placeholder: "Filter..." },
          on: { input: event => { filterFunction = event.target.value } },
        }),
      ]),
      button("Apply", _ => renderPage(), { attrs: {} }),
      button("Clear", _ => { filterStatus = ""; filterFunction = ""; renderPage() }, { attrs: {} }),
    ]),
    h("table", { attrs: { role: "grid" }, class: { "job-table": true } }, [
      h("thead", h("tr", [h("th", "Key"), ...job_headers()])),
      h("tbody", jobs.map(job =>
        h("tr", [
          link({ props: { href: root_path + "/queues/" + queue_name + "/jobs/" + job.key } }, h("td", job.key)),
          ...job_columns(queue_name, job),
        ])
      )),
    ]),
  ])
}

const job_view = function(data, queue_name, job_key) {
  const job = data.job
  const buttons = [button(
    "Retry",
    event => post(root_path + "/queues/" + queue_name + "/jobs/" + job_key + "/retry"),
    { style: { marginRight: "1rem" } },
  )]

  if (!job.completed) {
    buttons.push(button(
      "Abort",
      event => post(root_path + "/queues/" + queue_name + "/jobs/" + job_key + "/abort"),
      { style: { borderColor: "#d81b60", backgroundColor: "#d81b60" } },
    ))
  }

  return h("div", [
    h("hgroup", [
      h("h1", "Job"),
      h("h2", job_key),
    ]),
    h("grid", buttons),
    h("figure", h("table", [
      h("thead", h("tr", [
        ...job_headers(),
        h("td", "Queue"),
        h("td", "Progress"),
        h("td", "Attempts"),
      ])),
      h("tbody", h("tr", [
        ...job_columns(queue_name, job),
        h("td", link({ props: { href: "/queues/" + job.queue } }, job.queue)),
        h("td", h("progress", { props: { value: job.progress || 0, max: 1.0 } })),
        h("td", job.attempts),
      ])),
    ])),
    h("details", { props: { open: true } }, [
      h("summary", "Result"),
      h("p", job.result),
    ]),
    h("details", { props: { open: true } }, [
      h("summary", "Error"),
      h("p", job.error),
    ]),
  ])
}

const error_view = function(error) {
  return h("div", [
    h("h1", "Error"),
    h("pre", { style: { padding: "1rem" } }, error),
  ])
}

const root_path_1 = root_path + '/'

let routes = {}
routes[root_path + '/'] = { view: home_view, data: "/queues" }
routes[root_path + '/queues/:queue_id'] = { view: queue_view }
routes[root_path + '/queues/:queue_id/jobs/:job_id'] = { view: job_view }

routes = Object.keys(routes)
  .sort(function(a, b) { return b.length - a.length; })
  .map(function(path) {
    return {
      path: new RegExp("^" + path.replace(/:[^\\s/]+/g, "([^\\/]+)") + "$"),
      view: routes[path].view,
      data: routes[path].data,
    };
  })

const page = async function(path) {
  path ||= window.location.pathname
  const route = routes.find(route => path.match(route.path))
  let view = error_view("404 not found")
  if (route) {
    const data = await get(route.data || path)
    const args = path.match(route.path).slice(1)
    if (data.error) {
      view = error_view(data.error)
    } else if (route.view === queue_view) {
      const filteredJobs = await fetchFilteredJobs(args[0])
      view = route.view(data, ...args, filteredJobs)
    } else {
      view = route.view(data, ...args)
    }
  }

  // WebSocket status indicator
  const wsDot = h("span", {
    class: { "ws-dot": true, connected: wsConnected, disconnected: !wsConnected },
  })

  return h("div", [
    h("nav.container", [
      h("ul", h("li", link({ props: { href: root_path + "/" } }, [h("strong", "SAQ"), wsDot]))),
      h("ul", [
        h("li", h("a", { props: { href: "https://saq-py.readthedocs.io" } }, "Docs")),
      ]),
    ]),
    h("main.container", view),
  ])
}

// --- WebSocket ---
const connectWS = function() {
  const wsUrl = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + root_path + "/ws"
  const ws = new WebSocket(wsUrl)

  ws.onopen = function() {
    wsConnected = true
    renderPage()
  }

  ws.onmessage = function(event) {
    renderPage()
  }

  ws.onclose = function() {
    wsConnected = false
    renderPage()
    setTimeout(connectWS, 3000)
  }

  ws.onerror = function() {
    ws.close()
  }
}

renderPage()
connectWS()
