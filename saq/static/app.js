const patch = snabbdom.init([
  snabbdom.attributesModule,
  snabbdom.eventListenersModule,
  snabbdom.propsModule,
  snabbdom.styleModule,
])

const h = snabbdom.h

let container = document.getElementById("app")

const render = function(vnode) {
  patch(container, vnode)
  container = vnode
}

const renderPage = _ => page().then(view => render(view))

window.addEventListener("popstate", event => renderPage())

const handle_error = function(error) {
  console.log(error)
  return {error: error.toString()}
}

const get = async function(path) {
  try {
    const response = await fetch("/api" + path)
    return await response.json()
  } catch (error) {
    return handle_error(error)
  }
}

const post = async function(path, data) {
  try {
    const response = await fetch(
      "/api" + path,
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

const link = function(data, children) {
  const handler = function(event) {
    event.preventDefault()
    const path = data.props.href
    page(path).then(view => render(view))
    window.history.pushState(null, null, path)
    event.target.blur()
  }

  return h("a", Object.assign({on: {click: handler}}, data), children)
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
      h("tbody", {attrs: {role: "grid"}}, data.queues.map(queue =>
        h("tr", [
          h("td", link({props: {href: "/queues/" + queue.name}}, queue.name)),
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
]

const job_columns = job => [
  h("td", job.function),
  h("td", JSON.stringify(job.kwargs)),
  h("td", format_time(job.queued)),
  h("td", format_time(job.started)),
  h("td", format_time(job.completed)),
  h("td", job.status),
]

const queue_view = function(data, queue_name) {
  const queue = data.queue

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
    h("table", {attrs: {role: "grid"}}, [
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
          h("td", worker.complete),
          h("td", worker.retried),
          h("td", worker.failed),
          h("td", worker.uptime / 1000),
        ])
      )),
    ]),
    h("h2", "Jobs"),
    h("table", {attrs: {role: "grid"}}, [
      h("thead", h("tr", [h("th", "Key"), ...job_headers()])),
      h("tbody", queue.jobs.map(job =>
        h("tr", [
          link({props: {href: "/jobs/" + job.key}}, h("td", job.key)),
          ...job_columns(job),
        ])
      )),
    ]),
  ])
}

const job_view = function(data, job_key) {
  const job = data.job
  const buttons = [button(
    "Retry",
    event => post("/jobs/" + job_key + "/retry"),
    {style: {marginRight: "1rem"}},
  )]

  if (!job.completed) {
    buttons.push(button(
      "Abort",
      event => post("/jobs/" + job_key + "/abort"),
      {style: {borderColor: "#d81b60", backgroundColor: "#d81b60"}},
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
        ...job_columns(job),
        h("td", link({props: {href: "/queue/" + job.queue}}, job.queue)),
        h("td", h("progress", {props: {value: job.progress || 0, max: 1.0}})),
        h("td", job.attempts),
      ])),
    ])),
    h("details", {props: {open: true}}, [
      h("summary", "Result"),
      h("p", JSON.stringify(job.result)),
    ]),
    h("details", {props: {open: true}}, [
      h("summary", "Error"),
      h("p", job.error),
    ]),
  ])
}

const error_view = function(error) {
  return h("div", [
    h("h1", "Error"),
    h("pre", {style: {padding: "1rem"}}, error),
  ])
}

let routes = {
  '/': {view: home_view, data: "/queues"},
  '/queues/:queue_id': {view: queue_view},
  '/jobs/:job_id': {view: job_view}
}

routes = Object.keys(routes)
  .sort(function(a, b){ return b.length - a.length; })
  .map(function(path) {
    return {
      path: new RegExp("^" + path.replace(/:[^\s/]+/g, "([\\w-]+)") + "$"),
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
    view = data.error ? error_view(data.error) : route.view(data, ...args)
  }

  return h("div", [
    h("nav.container", [
      h("ul", h("li", link({props: {href: "/"}}, h("strong", "SAQ")))),
      h("ul", [
        h("li", h("a", {props: {href: "https://github.com/tobymao/saq"}}, "Docs")),
      ]),
    ]),
    h("main.container", view),
  ])
}

renderPage()
setInterval(_ => renderPage(), 2000)
