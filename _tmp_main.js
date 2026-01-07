(function () {
  const content = document.getElementById("content-area");
  const sidebar = document.getElementById("sidebar");
  const toggle = document.getElementById("sidebar-toggle");
  const logoutBtn = document.getElementById("logout-btn");
  const menu = document.getElementById("menu");
  const userMeta = document.getElementById("user-meta");
  const userPerfilId = userMeta ? userMeta.dataset.perfilId : "";
  const userNivel = userMeta ? userMeta.dataset.nivel : "";
  const themeLightBtn = document.getElementById("theme-light");
  const themeDarkBtn = document.getElementById("theme-dark");
  let multiFilterClickBound = false;

  function applyTheme(theme) {
    const body = document.body;
    const isDark = theme === "dark";
    body.classList.toggle("theme-dark", isDark);
    if (themeLightBtn && themeDarkBtn) {
      themeLightBtn.classList.toggle("active", !isDark);
      themeDarkBtn.classList.toggle("active", isDark);
    }
    localStorage.setItem("app-theme", isDark ? "dark" : "light");
  }

  function initTheme() {
    const saved = localStorage.getItem("app-theme") || "light";
    applyTheme(saved);
    if (themeLightBtn) {
      themeLightBtn.addEventListener("click", () => applyTheme("light"));
    }
    if (themeDarkBtn) {
      themeDarkBtn.addEventListener("click", () => applyTheme("dark"));
    }
  }

  function bindToggleVisibility(scope) {
    scope.querySelectorAll(".toggle-visibility").forEach((btn) => {
      const targetId = btn.getAttribute("data-target");
      const target = targetId ? document.getElementById(targetId) : null;
      if (!target) return;
      btn.addEventListener("click", () => {
        const isPwd = target.type === "password";
        target.type = isPwd ? "text" : "password";
        btn.innerHTML = `<i class="bi ${isPwd ? "bi-eye-slash" : "bi-eye"}"></i>`;
      });
    });
  }

  async function loadPage(route) {
    let url = "/partial/" + route;
    if (route === "logout") {
      await logout();
      return;
    }
    try {
      const res = await fetch(url, { headers: { "X-Requested-With": "fetch" } });
      if (res.status === 401) {
        window.location.href = "/login";
        return;
      }
      if (res.status === 403) {
        content.innerHTML = '<div class="card"><div class="card-title">Acesso negado</div><p>Requer perfil admin.</p></div>';
        return;
      }
      const html = await res.text();
      content.innerHTML = html;
      initRoute(route);
    } catch (err) {
      content.innerHTML = '<div class="card"><div class="card-title">Erro</div><p>Falha ao carregar.</p></div>';
      console.error(err);
    }
  }

  function setActive(route) {
    document.querySelectorAll(".menu-item").forEach((el) => {
      const r = el.getAttribute("data-route");
      if (r === route) {
        el.classList.add("active");
      } else {
        el.classList.remove("active");
      }
    });

    // expand parent submenu for active route
    document.querySelectorAll(".menu-group").forEach((group) => {
      const submenu = group.querySelector(".submenu");
      if (!submenu) return;
      const hasActive = Array.from(submenu.querySelectorAll("[data-route]")).some(
        (item) => item.getAttribute("data-route") === route
      );
      group.classList.toggle("open", hasActive);
    });
  }

  async function logout() {
    try {
      await fetch("/logout", { method: "POST" });
    } finally {
      window.location.href = "/login";
    }
  }

  function updateToggleIcon() {
    if (!toggle || !sidebar) return;
    const icon = toggle.querySelector("i");
    if (!icon) return;
    const collapsed = sidebar.classList.contains("collapsed");
    icon.classList.toggle("bi-chevron-right", collapsed);
    icon.classList.toggle("bi-chevron-left", !collapsed);
  }

  function setUserMeta() {
    if (!userMeta) return;
    const name = userMeta.dataset.name || "";
    const activeCount = userMeta.dataset.activeCount || "";
    const initialFeats = userMeta.dataset.features
      ? JSON.parse(userMeta.dataset.features || "[]")
      : [];
    if (initialFeats.length) {
      applyMenuPermissions(initialFeats);
    }
    const formatted = new Date().toLocaleString("pt-BR", {
      dateStyle: "short",
      timeStyle: "short",
    });
    const countLabel = activeCount ? ` | Logados: ${activeCount}` : "";
    userMeta.textContent = `${name} - ${formatted}${countLabel}`;
  }

  if (toggle) {
    toggle.addEventListener("click", () => {
      sidebar.classList.toggle("collapsed");
      sidebar.classList.toggle("open");
      updateToggleIcon();
    });
    updateToggleIcon();
  }

  if (logoutBtn) {
    logoutBtn.addEventListener("click", () => logout());
  }

  initTheme();
  function initUsuariosForm() {
    const form = document.getElementById("form-criar-usuario");
    const msg = document.getElementById("criar-usuario-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    bindToggleVisibility(form);

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const data = Object.fromEntries(new FormData(form));
      data.ativo = !!data.ativo;
      try {
        const res = await fetch("/api/usuarios", {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(data),
        });
        const payload = await res.json();
        if (!res.ok) {
          msg.textContent = payload.error || "Erro ao salvar.";
          msg.classList.add("text-error");
          return;
        }
        msg.textContent = "Usuario criado.";
        form.reset();
        await loadPage("usuarios");
      } catch (err) {
        console.error(err);
        msg.textContent = "Falha na requisicao.";
        msg.classList.add("text-error");
      }
    });
  }

  function initUsuariosEditar() {
    const form = document.getElementById("form-editar-usuario");
    const msg = document.getElementById("editar-usuario-msg");
    const fillFromRow = (row) => {
      const email = row.dataset.email || "";
      document.getElementById("edit-email").value = email;
      document.getElementById("edit-email-display").value = email;
      document.getElementById("edit-nome").value = row.dataset.nome || "";
      document.getElementById("edit-perfil").value = row.dataset.perfil || "";
      document.getElementById("edit-senha").value = "";
      document.getElementById("edit-ativo").checked = row.dataset.ativo === "1";
    };
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    bindToggleVisibility(form);

    document.querySelectorAll(".select-usuario").forEach((btn) => {
      btn.addEventListener("click", () => {
        const row = btn.closest("tr[data-email]");
        if (row) fillFromRow(row);
      });
    });

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const email = document.getElementById("edit-email").value;
      if (!email) {
        msg.textContent = "Selecione um usuario na lista.";
        msg.classList.add("text-error");
        return;
      }
      const payload = {
        nome: document.getElementById("edit-nome").value,
        perfil: document.getElementById("edit-perfil").value,
        senha: document.getElementById("edit-senha").value,
        ativo: document.getElementById("edit-ativo").checked,
      };
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const raw = await res.text();
        let data = {};
        try {
          data = JSON.parse(raw || "{}");
        } catch {
          // se nÒo for JSON, usa texto bruto na mensagem de erro
        }
        if (!res.ok) throw new Error(data.error || raw || `Falha ao salvar. Status ${res.status}`);
        msg.textContent = data.message || "Usuario atualizado.";
        document.getElementById("edit-senha").value = "";
        const row = document.querySelector(`tr[data-email="${email}"]`);
        if (row) {
          row.dataset.nome = payload.nome || row.dataset.nome || "";
          row.dataset.perfil = payload.perfil || row.dataset.perfil || "";
          row.dataset.ativo = payload.ativo ? "1" : "0";
          const cells = row.querySelectorAll("td");
          if (cells.length >= 4) {
            cells[1].textContent = payload.nome || cells[1].textContent;
            cells[2].textContent = payload.perfil || cells[2].textContent;
            cells[3].textContent = payload.ativo ? "Sim" : "Nao";
          }
        }
      } catch (err) {
        console.error(err);
        msg.textContent = err.message;
        msg.classList.add("text-error");
      }
    });
  }

  function initPerfis() {
    const form = document.getElementById("form-perfil");
    const msg = document.getElementById("perfil-msg");
    if (!form || !msg) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";

    const fillForm = (row) => {
      document.getElementById("perfil-id").value = row?.dataset.id || "";
      document.getElementById("perfil-nome").value = row?.dataset.nome || "";
      document.getElementById("perfil-nivel").value = row?.dataset.nivel || "";
      document.getElementById("perfil-ativo").checked = (row?.dataset.ativo || "1") === "1";
    };

    document.querySelectorAll(".select-perfil").forEach((btn) => {
      btn.addEventListener("click", () => {
        const row = btn.closest("tr[data-id]");
        if (row) fillForm(row);
      });
    });
    document.querySelectorAll(".delete-perfil").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.dataset.id;
        if (!id) return;
        msg.textContent = "Excluindo...";
        msg.classList.remove("text-error");
        try {
          const res = await fetch(`/api/perfis/${id}`, {
            method: "DELETE",
            headers: { "X-Requested-With": "fetch" },
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || "Falha ao excluir.");
          msg.textContent = data.message || "Perfil excluido.";
          loadPage("usuarios/perfil");
        } catch (err) {
          console.error(err);
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
      });
    });

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      msg.textContent = "Salvando...";
      msg.classList.remove("text-error");
      const id = document.getElementById("perfil-id").value;
      const payload = {
        nome: document.getElementById("perfil-nome").value,
        nivel: document.getElementById("perfil-nivel").value,
        ativo: document.getElementById("perfil-ativo").checked,
      };
      const url = id ? `/api/perfis/${id}` : "/api/perfis";
      const method = id ? "PUT" : "POST";
      try {
        const res = await fetch(url, {
          method,
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        msg.textContent = data.message || "Perfil salvo.";
        loadPage("usuarios/perfil");
      } catch (err) {
        console.error(err);
        msg.textContent = err.message;
        msg.classList.add("text-error");
      }
    });
  }

  function applyMenuPermissions(features = []) {
    if (!menu) return;
    const allowed = new Set(["dashboard", "logout", ...features]);

    // Children: show only allowed
    menu.querySelectorAll(".submenu [data-route]").forEach((link) => {
      const route = link.getAttribute("data-route");
      if (!route) return;
      link.style.display = allowed.has(route) ? "" : "none";
    });

    // Parents: show if any allowed child
    menu.querySelectorAll(".menu-group").forEach((group) => {
      const submenu = group.querySelector(".submenu");
      if (!submenu) return;
      const parentId = group.id?.replace("menu-", "") || "";
      const hasAllowedChild = Array.from(submenu.querySelectorAll("[data-route]")).some((item) =>
        allowed.has(item.getAttribute("data-route"))
      );
      const parentAllowed = parentId && allowed.has(parentId);
      group.style.display = hasAllowedChild || parentAllowed ? "" : "none";
    });

    // Top-level items without submenu
    menu.querySelectorAll(".menu > .menu-item[data-route]").forEach((item) => {
      const route = item.getAttribute("data-route");
      if (!route) return;
      if (route === "logout") return;
      item.style.display = allowed.has(route) ? "" : "none";
    });
  }

  async function fetchCurrentPermissions() {
    if (userNivel === "1") {
      // admin: libera tudo visível no menu
      const allRoutes = Array.from(menu.querySelectorAll("[data-route]")).map((el) =>
        el.getAttribute("data-route")
      );
      applyMenuPermissions(allRoutes);
      return;
    }
    try {
      const res = await fetch("/api/permissoes/current", {
        headers: { "X-Requested-With": "fetch" },
      });
      if (!res.ok) return;
      const data = await res.json();
      const feats = data.features || [];
      const locked = ["dashboard", "logout"];
      applyMenuPermissions(feats);
    } catch (err) {
      console.error(err);
    }
  }

  function initPainel() {
    const dataScript = document.getElementById("painel-data");
    const treeEl = document.getElementById("painel-tree");
    const ativosEl = document.getElementById("painel-ativos");
    const selectPerfil = document.getElementById("painel-perfil");
    const btnSalvar = document.getElementById("painel-salvar");
    const btnCancelar = document.getElementById("painel-cancelar");
    const msg = document.getElementById("painel-msg");
    if (!dataScript || !treeEl || !ativosEl || !selectPerfil) return;
    if (treeEl.dataset.bound === "1") return;
    treeEl.dataset.bound = "1";

    const features = JSON.parse(dataScript.dataset.features || "[]");
    const allowedRaw = JSON.parse(dataScript.dataset.allowed || "{}");
    const allowed = {};
    Object.entries(allowedRaw).forEach(([k, v]) => {
      allowed[String(k)] = v;
    });
    const locked = new Set(features.filter((f) => f.locked).map((f) => f.id));
    const sortFeatures = (items) =>
      (items || [])
        .map((f) => ({
          ...f,
          children: f.children ? sortFeatures([...f.children]) : [],
        }))
        .sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR", { sensitivity: "base" }));
    const sortedFeatures = sortFeatures(features);
    let original = {};
    Object.entries(allowed).forEach(([k, v]) => {
      original[k] = [...v];
    });

    const renderAtivos = (list) => {
      ativosEl.innerHTML = "";
      list.forEach((item) => {
        const li = document.createElement("li");
        li.textContent = item;
        ativosEl.appendChild(li);
      });
    };

    const buildTree = (perfil) => {
      treeEl.innerHTML = "";
      const currentAllowed = new Set(allowed[perfil] || []);
      locked.forEach((f) => currentAllowed.add(f));

      const toggleChildren = (node, checked) => {
        node.querySelectorAll("input[type='checkbox']").forEach((cb) => {
          cb.checked = checked;
          const id = cb.dataset.id;
          if (locked.has(id)) return;
          if (checked) currentAllowed.add(id);
          else currentAllowed.delete(id);
        });
      };

      const createNode = (feat) => {
        const wrapper = document.createElement("div");
        wrapper.className = "tree-item";
        const controls = document.createElement("div");
        controls.className = "tree-controls";
        if (feat.children && feat.children.length) {
          const toggleBtn = document.createElement("button");
          toggleBtn.type = "button";
          toggleBtn.className = "tree-toggle";
          const startCollapsed = true;
          if (startCollapsed) wrapper.classList.add("collapsed");
          toggleBtn.innerHTML = `<i class="bi bi-caret-${startCollapsed ? "right" : "down"}-fill"></i>`;
          toggleBtn.addEventListener("click", () => {
            const collapsed = wrapper.classList.toggle("collapsed");
            toggleBtn.innerHTML = `<i class="bi bi-caret-${collapsed ? "right" : "down"}-fill"></i>`;
            const childBox = wrapper.querySelector(".tree-children");
            if (childBox) childBox.style.display = collapsed ? "none" : "flex";
          });
          controls.appendChild(toggleBtn);
        } else {
          const spacer = document.createElement("span");
          spacer.style.display = "inline-block";
          spacer.style.width = "14px";
          controls.appendChild(spacer);
        }
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = currentAllowed.has(feat.id);
        cb.dataset.id = feat.id;
        cb.disabled = locked.has(feat.id);
        controls.appendChild(cb);
        const label = document.createElement("span");
        label.textContent = feat.nome;
        wrapper.appendChild(controls);
        wrapper.appendChild(label);

        cb.addEventListener("change", () => {
          if (cb.checked) {
            currentAllowed.add(feat.id);
            if (feat.parentId) {
              const parentCb = treeEl.querySelector(`input[data-id='${feat.parentId}']`);
              if (parentCb) {
                parentCb.checked = true;
                currentAllowed.add(feat.parentId);
              }
            }
          } else {
            if (!locked.has(feat.id)) currentAllowed.delete(feat.id);
            // desmarca filhos
            if (feat.children && feat.children.length) {
              const subtree = wrapper.querySelector(".tree-children");
              if (subtree) toggleChildren(subtree, false);
            }
          }
          allowed[perfil] = Array.from(currentAllowed);
          renderAtivos(allowed[perfil]);
        });

        if (feat.children && feat.children.length) {
          const childrenBox = document.createElement("div");
          childrenBox.className = "tree-children";
          if (wrapper.classList.contains("collapsed")) {
            childrenBox.style.display = "none";
          }
          feat.children.forEach((ch) => {
            ch.parentId = feat.id;
            const childNode = createNode(ch);
            childrenBox.appendChild(childNode);
          });
          wrapper.appendChild(childrenBox);
        }
        return wrapper;
      };

      sortedFeatures.forEach((f) => {
        const node = createNode(f);
        treeEl.appendChild(node);
      });
      renderAtivos(allowed[perfil] || []);
    };

    const loadPerfilPermissions = async (perfil) => {
      if (!perfil) return [];
      try {
        const res = await fetch(`/api/permissoes/${perfil}`, { headers: { "X-Requested-With": "fetch" } });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar permissoes.");
        return Array.isArray(data.features) ? data.features : [];
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        return [];
      }
    };

    selectPerfil.addEventListener("change", async () => {
      const perfil = String(selectPerfil.value || "");
      if (!perfil) {
        treeEl.innerHTML = "";
        ativosEl.innerHTML = "";
        return;
      }
      // sempre recarrega do backend para refletir banco
      const feats = await loadPerfilPermissions(perfil);
      allowed[perfil] = feats.filter((f) => typeof f === "string");
      locked.forEach((f) => {
        if (!allowed[perfil].includes(f)) allowed[perfil].push(f);
      });
      buildTree(perfil);
    });

    const handleSalvar = async () => {
      const perfil = String(selectPerfil.value || "");
      if (!perfil) {
        if (msg) msg.textContent = "Selecione um perfil.";
        return;
      }
      const feats = allowed[perfil] || [];
      locked.forEach((f) => {
        if (!feats.includes(f)) feats.push(f);
      });
      if (msg) msg.textContent = "Salvando...";
      try {
        const res = await fetch(`/api/permissoes/${perfil}`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify({ features: feats }),
        });
        if (!res.ok) {
          const text = await res.text();
          throw new Error(text || `Erro ${res.status}`);
        }
        const data = await res.json();
        original[perfil] = [...feats];
        if (msg) msg.textContent = data.message || "Permissões salvas.";
      } catch (err) {
        console.error(err);
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
      }
    };

    const handleCancelar = () => {
      const perfil = selectPerfil.value;
      if (!perfil) return;
      allowed[perfil] = [...(original[perfil] || [])];
      buildTree(perfil);
      if (msg) {
        msg.textContent = "";
        msg.classList.remove("text-error");
      }
    };

    if (btnSalvar) btnSalvar.addEventListener("click", handleSalvar);
    if (btnCancelar) btnCancelar.addEventListener("click", handleCancelar);
  }

  function initUsuariosSenha() {
    const formBuscar = document.getElementById("form-buscar-usuario");
    const formAlterar = document.getElementById("form-alterar-senha");
    const areaSenha = document.getElementById("senha-area");
    const msgBuscar = document.getElementById("buscar-usuario-msg");
    const msgSenha = document.getElementById("senha-msg");
    const btnCancelar = document.getElementById("senha-cancelar");
    if (!formBuscar || !formAlterar || !areaSenha) return;
    if (formBuscar.dataset.bound === "1") return;
    formBuscar.dataset.bound = "1";
    bindToggleVisibility(formAlterar);

    const fillUser = (data) => {
      document.getElementById("senha-email").value = data.email || "";
      document.getElementById("senha-nome").value = data.nome || "";
      document.getElementById("senha-perfil").value = data.perfil || "";
      document.getElementById("senha-atual").value = "";
      document.getElementById("senha-nova").value = "";
      document.getElementById("senha-confirmar").value = "";
      areaSenha.style.display = "block";
      if (msgBuscar) msgBuscar.textContent = "";
    };

    formBuscar.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const email = (document.getElementById("buscar-email").value || "").trim();
      if (!email) return;
      if (msgBuscar) msgBuscar.textContent = "Consultando...";
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}`, {
          headers: { "X-Requested-With": "fetch" },
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao consultar.");
        fillUser(data);
      } catch (err) {
        console.error(err);
        if (msgBuscar) {
          msgBuscar.textContent = err.message;
          msgBuscar.classList.add("text-error");
        }
        areaSenha.style.display = "none";
      }
    });

    formAlterar.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const email = document.getElementById("senha-email").value;
      if (!email) return;
      if (msgSenha) {
        msgSenha.textContent = "Salvando...";
        msgSenha.classList.remove("text-error");
      }
      const payload = {
        senha_atual: document.getElementById("senha-atual").value,
        senha_nova: document.getElementById("senha-nova").value,
        senha_confirmar: document.getElementById("senha-confirmar").value,
      };
      try {
        const res = await fetch(`/api/usuarios/${encodeURIComponent(email)}/senha`, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-Requested-With": "fetch" },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao salvar.");
        if (msgSenha) msgSenha.textContent = data.message || "Senha atualizada.";
        formAlterar.reset();
      } catch (err) {
        console.error(err);
        if (msgSenha) {
          msgSenha.textContent = err.message;
          msgSenha.classList.add("text-error");
        }
      }
    });

    if (btnCancelar) {
      btnCancelar.addEventListener("click", () => {
        formAlterar.reset();
        areaSenha.style.display = "none";
        if (msgSenha) {
          msgSenha.textContent = "";
          msgSenha.classList.remove("text-error");
        }
      });
    }
  }

  async function loadFipStatus(target) {
    if (!target) return;
    target.textContent = "Carregando...";
    try {
      const res = await fetch("/api/fip613/status");
      if (!res.ok) throw new Error("Erro ao consultar status");
      const data = await res.json();
      if (!data.last) {
        target.textContent = "Nenhuma atualização encontrada.";
        return;
      }
      const last = data.last;
      const uploaded = last.uploaded_at ? new Date(last.uploaded_at).toLocaleString("pt-BR") : "-";
      const dataArquivo = last.data_arquivo ? new Date(last.data_arquivo).toLocaleString("pt-BR") : "-";
      target.innerHTML = `
        <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
        <div><strong>Upload em:</strong> ${uploaded}</div>
        <div><strong>Data do download:</strong> ${dataArquivo}</div>
        <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
        <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
      `;
    } catch (err) {
      target.textContent = "Falha ao carregar status.";
      console.error(err);
    }
  }

  function setDefaultAmazonTime(input) {
    if (!input) return;
    const now = new Date();
    const parts = new Intl.DateTimeFormat("sv-SE", {
      timeZone: "America/Manaus",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    })
      .formatToParts(now)
      .reduce((acc, p) => ({ ...acc, [p.type]: p.value }), {});
    input.value = `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}`;
  }

  function initFip613() {
    const form = document.getElementById("form-fip613");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("fip613-msg");
    const statusBox = document.getElementById("fip613-status");
    const inputData = document.getElementById("fip613-data");
    const fileInput = document.getElementById("fip613-file");
    const loading = document.getElementById("fip613-loading");
  const submitBtn = document.getElementById("fip613-submit");
  const defaultLabel = "Upload e processar";
  const viewLabel = "Ver Relatorio";

  if (inputData) {
    setDefaultAmazonTime(inputData);
  }

    loadFipStatus(statusBox);

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view") {
          ev.preventDefault();
          ev.stopPropagation();
          setActive("relatorios/fip613");
          loadPage("relatorios/fip613");
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view") {
        setActive("relatorios/fip613");
        loadPage("relatorios/fip613");
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/fip613/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluído.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        loadFipStatus(statusBox);
        if (submitBtn) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });
  }

  function initPlan20() {
    const form = document.getElementById("form-plan20");
    if (!form) return;
    if (form.dataset.bound === "1") return;
    form.dataset.bound = "1";
    const msg = document.getElementById("plan20-msg");
    const statusBox = document.getElementById("plan20-status");
    const inputData = document.getElementById("plan20-data");
    const fileInput = document.getElementById("plan20-file");
    const loading = document.getElementById("plan20-loading");
  const submitBtn = document.getElementById("plan20-submit");
  const defaultLabel = "Upload e processar";
  const viewLabel = "Ver saída";

  if (inputData) {
    setDefaultAmazonTime(inputData);
  }

    const loadStatus = async () => {
      if (!statusBox) return;
      statusBox.textContent = "Carregando...";
      try {
        const res = await fetch("/api/plan20/status");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Erro ao consultar status");
        if (!data.last) {
          statusBox.textContent = "Nenhuma atualização encontrada.";
          return;
        }
        const last = data.last;
        const uploaded = last.uploaded_at ? new Date(last.uploaded_at).toLocaleString("pt-BR") : "-";
        const dataArquivo = last.data_arquivo ? new Date(last.data_arquivo).toLocaleString("pt-BR") : "-";
        statusBox.innerHTML = `
          <div><strong>Enviado por:</strong> ${last.user_email || "-"}</div>
          <div><strong>Upload em:</strong> ${uploaded}</div>
          <div><strong>Data do download:</strong> ${dataArquivo}</div>
          <div><strong>Arquivo original:</strong> ${last.original_filename || "-"}</div>
          <div><strong>Saída gerada:</strong> ${last.output_filename || "-"}</div>
        `;
        if (submitBtn && data.last && data.last.output_filename) {
          submitBtn.dataset.mode = "view";
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.output = data.last.output_filename;
        }
      } catch (err) {
        statusBox.textContent = "Falha ao carregar status.";
        console.error(err);
      }
    };

    if (submitBtn) {
      submitBtn.dataset.mode = "upload";
      submitBtn.textContent = defaultLabel;
      submitBtn.addEventListener("click", (ev) => {
        if (submitBtn.dataset.mode === "view" && submitBtn.dataset.output) {
          ev.preventDefault();
          window.open(`/api/plan20/download/${encodeURIComponent(submitBtn.dataset.output)}`, "_blank");
        }
      });
    }

    if (fileInput && submitBtn) {
      fileInput.addEventListener("change", () => {
        submitBtn.dataset.mode = "upload";
        submitBtn.textContent = defaultLabel;
      });
    }

    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (submitBtn?.dataset.mode === "view" && submitBtn.dataset.output) {
        window.open(`/api/plan20/download/${encodeURIComponent(submitBtn.dataset.output)}`, "_blank");
        return;
      }
      if (!fileInput?.files?.length) {
        if (msg) msg.textContent = "Selecione um arquivo .xlsx.";
        return;
      }
      if (loading) loading.style.display = "inline";
      if (submitBtn) submitBtn.disabled = true;
      const fd = new FormData(form);
      try {
        const res = await fetch("/api/plan20/upload", {
          method: "POST",
          body: fd,
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao enviar.");
        if (msg) {
          msg.textContent = data.message || "Upload concluído.";
          msg.classList.remove("text-error");
        }
        form.reset();
        if (inputData) inputData.value = "";
        await loadStatus();
        if (submitBtn && data.output) {
          submitBtn.textContent = viewLabel;
          submitBtn.dataset.mode = "view";
          submitBtn.dataset.output = data.output;
        }
      } catch (err) {
        if (msg) {
          msg.textContent = err.message;
          msg.classList.add("text-error");
        }
        console.error(err);
        if (submitBtn) {
          submitBtn.textContent = defaultLabel;
          submitBtn.dataset.mode = "upload";
        }
      } finally {
        if (loading) loading.style.display = "none";
        if (submitBtn) submitBtn.disabled = false;
      }
    });

    loadStatus();
  }

  function initRelatorioFip() {
    const table = document.getElementById("fip613-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const meta = document.getElementById("fip613-relatorio-meta");
    const pager = document.getElementById("fip613-pagination");
    const pageSizeSelect = document.getElementById("fip613-page-size");
    const btnDownload = document.getElementById("fip613-download");
    const btnReset = document.getElementById("fip613-reset");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];
    const sumCols = [
      "dotacao_inicial",
      "cred_suplementar",
      "cred_especial",
      "cred_extraordinario",
      "reducao",
      "cred_autorizado",
      "bloqueado_conting",
      "reserva_empenho",
      "saldo_destaque",
      "saldo_dotacao",
      "empenhado",
      "liquidado",
      "a_liquidar",
      "valor_pago",
      "valor_a_pagar",
    ];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmt = (v) => {
      const n = Number(v || 0);
      if (Object.is(n, -0)) return "-";
      return n === 0 ? "-" : numFmt.format(n);
    };
    const numCls = (v) => {
      const n = Number(v || 0);
      const classes = ["num"];
      if (n > 0) classes.push("pos");
      else if (n < 0) classes.push("neg");
      return classes.join(" ");
    };

    const computeTotals = (rows) => {
      const totals = Object.fromEntries(sumCols.map((c) => [c, 0]));
      const paoeSet = new Set();
      const grupoSet = new Set();
      rows.forEach((r) => {
        const paoeParts = (r.projeto_atividade || "")
          .split(/\s+/)
          .filter((p) => /^\d+$/.test(p));
        if (paoeParts.length) paoeSet.add(paoeParts.join("*"));
        const natStr = String(r.natureza_despesa || "");
        if (natStr.length >= 2) grupoSet.add(natStr[1]);
        sumCols.forEach((c) => {
          const v = Number(r[c] || 0);
          if (!Number.isNaN(v)) totals[c] += v;
        });
      });
      return { totals, paoeSet, grupoSet };
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderFiltered(false);
        });
        pager.appendChild(b);
      };
      addBtn("«", 1, currentPage === 1);
      addBtn("‹", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "…";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn("›", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn("»", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const viewRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      const adjustedRows = rows.map((r) => {
        const copy = { ...r };
        negateCols.forEach((k) => {
          copy[k] = adjustVal(k, copy[k]);
        });
        return copy;
      });
      const { totals, paoeSet, grupoSet } = computeTotals(adjustedRows);
      const pageRows = adjustedRows.slice(startIdx, startIdx + pageSize);
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.uo || ""}</td>
          <td>${r.ug || ""}</td>
          <td>${r.funcao || ""}</td>
          <td>${r.subfuncao || ""}</td>
          <td>${r.programa || ""}</td>
          <td>${r.projeto_atividade || ""}</td>
          <td>${r.regional || ""}</td>
          <td>${r.natureza_despesa || ""}</td>
          <td>${r.fonte_recurso || ""}</td>
          <td>${r.iduso ?? ""}</td>
          <td>${r.tipo_recurso || ""}</td>
          <td class="${numCls(r.dotacao_inicial)}">${fmt(r.dotacao_inicial)}</td>
          <td class="${numCls(r.cred_suplementar)}">${fmt(r.cred_suplementar)}</td>
          <td class="${numCls(r.cred_especial)}">${fmt(r.cred_especial)}</td>
          <td class="${numCls(r.cred_extraordinario)}">${fmt(r.cred_extraordinario)}</td>
          <td class="${numCls(r.reducao)}">${fmt(r.reducao)}</td>
          <td class="${numCls(r.cred_autorizado)}">${fmt(r.cred_autorizado)}</td>
          <td class="${numCls(r.bloqueado_conting)}">${fmt(r.bloqueado_conting)}</td>
          <td class="${numCls(r.reserva_empenho)}">${fmt(r.reserva_empenho)}</td>
          <td class="${numCls(r.saldo_destaque)}">${fmt(r.saldo_destaque)}</td>
          <td class="${numCls(r.saldo_dotacao)}">${fmt(r.saldo_dotacao)}</td>
          <td class="${numCls(r.empenhado)}">${fmt(r.empenhado)}</td>
          <td class="${numCls(r.liquidado)}">${fmt(r.liquidado)}</td>
          <td class="${numCls(r.a_liquidar)}">${fmt(r.a_liquidar)}</td>
          <td class="${numCls(r.valor_pago)}">${fmt(r.valor_pago)}</td>
          <td class="${numCls(r.valor_a_pagar)}">${fmt(r.valor_a_pagar)}</td>
        `;
        tbody.appendChild(tr);
      });
      // linha de totais
      const totalTr = document.createElement("tr");
      totalTr.innerHTML = `
        <td colspan="11"><strong>Totais (linhas filtradas)</strong></td>
        <td class="${numCls(totals.dotacao_inicial)}"><strong>${totals.dotacao_inicial.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_suplementar)}"><strong>${totals.cred_suplementar.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_especial)}"><strong>${totals.cred_especial.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_extraordinario)}"><strong>${totals.cred_extraordinario.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.reducao)}"><strong>${totals.reducao.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.cred_autorizado)}"><strong>${totals.cred_autorizado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.bloqueado_conting)}"><strong>${totals.bloqueado_conting.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.reserva_empenho)}"><strong>${totals.reserva_empenho.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.saldo_destaque)}"><strong>${totals.saldo_destaque.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.saldo_dotacao)}"><strong>${totals.saldo_dotacao.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.empenhado)}"><strong>${totals.empenhado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.liquidado)}"><strong>${totals.liquidado.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.a_liquidar)}"><strong>${totals.a_liquidar.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.valor_pago)}"><strong>${totals.valor_pago.toLocaleString("pt-BR")}</strong></td>
        <td class="${numCls(totals.valor_a_pagar)}"><strong>${totals.valor_a_pagar.toLocaleString("pt-BR")}</strong></td>
      `;
      tbody.appendChild(totalTr);

      const paoeEl = document.getElementById("tot-paoe");
      const grupoEl = document.getElementById("tot-grupo");
      const credAutoEl = document.getElementById("tot-cred-autorizado");
      const bloqueadoEl = document.getElementById("tot-bloqueado");
      const tetoEl = document.getElementById("tot-teto");
      const saldoDotEl = document.getElementById("tot-saldo-dotacao");
      if (paoeEl) {
        if (paoeSet.size === 0) {
          paoeEl.textContent = "-";
        } else if (paoeSet.size > 10) {
          paoeEl.textContent = "Vários PAOEs";
        } else {
          paoeEl.textContent = Array.from(paoeSet).join(" * ");
        }
      }
      if (grupoEl) grupoEl.textContent = grupoSet.size ? Array.from(grupoSet).join("*") : "-";
      const formatVal = (el, val) => {
        if (!el) return;
        const n = Number(val || 0);
        el.textContent = n === 0 ? "-" : n.toLocaleString("pt-BR");
        el.classList.remove("pos", "neg");
        if (n > 0) el.classList.add("pos");
        if (n < 0) el.classList.add("neg");
      };
      formatVal(credAutoEl, totals.cred_autorizado);
      const bloqueadoVal = totals.bloqueado_conting;
      // bloquear cores no cred_autorizado e teto
      if (bloqueadoEl) formatVal(bloqueadoEl, bloqueadoVal);
      if (tetoEl) {
        const teto = totals.cred_autorizado + bloqueadoVal;
        tetoEl.textContent = Number(teto || 0).toLocaleString("pt-BR");
        tetoEl.classList.remove("pos", "neg");
      }
      if (credAutoEl) {
        credAutoEl.textContent = Number(totals.cred_autorizado || 0).toLocaleString("pt-BR");
        credAutoEl.classList.remove("pos", "neg");
      }
      formatVal(saldoDotEl, totals.saldo_dotacao);
      renderPagination(totalPages);
    };

    const allData = { rows: [] };

    const colKeys = [
      "uo",
      "ug",
      "funcao",
      "subfuncao",
      "programa",
      "projeto_atividade",
      "regional",
      "natureza_despesa",
      "fonte_recurso",
      "iduso",
      "tipo_recurso",
      "dotacao_inicial",
      "cred_suplementar",
      "cred_especial",
      "cred_extraordinario",
      "reducao",
      "cred_autorizado",
      "bloqueado_conting",
      "reserva_empenho",
      "saldo_destaque",
      "saldo_dotacao",
      "empenhado",
      "liquidado",
      "a_liquidar",
      "valor_pago",
      "valor_a_pagar",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

      if (!multiFilterClickBound) {
        document.addEventListener("click", (ev) => {
          if (!ev.target.closest(".mf-wrapper")) {
            closeAllPanels();
          }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/fip613");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = data.data_arquivo ? new Date(data.data_arquivo).toLocaleString("pt-BR") : "-";
          const user = data.user_email || "-";
          const uploaded = data.uploaded_at ? new Date(data.uploaded_at).toLocaleString("pt-BR") : "-";
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/fip613/download", "_blank");
      });
    }
  }

  function initRelatorioPlan20() {
    const table = document.getElementById("plan20-relatorio-tabela");
    const tbody = table ? table.querySelector("tbody") : null;
    const meta = document.getElementById("plan20-relatorio-meta");
    const pager = document.getElementById("plan20-pagination");
    const pageSizeSelect = document.getElementById("plan20-page-size");
    const btnDownload = document.getElementById("plan20-download");
    const btnReset = document.getElementById("plan20-reset");
    const totExercicio = document.getElementById("plan20-tot-exercicio");
    const totValorTotal = document.getElementById("plan20-tot-valor-total");
    if (!table || !tbody) return;
    if (table.dataset.bound === "1") return;
    table.dataset.bound = "1";

    let pageSize = parseInt(pageSizeSelect?.value || "20", 10) || 20;
    let currentPage = 1;
    let filteredRows = [];

    const numFmt = new Intl.NumberFormat("pt-BR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const fmtNum = (v) => {
      const n = Number(v);
      if (Number.isNaN(n)) return v ?? "";
      return numFmt.format(n);
    };

    const updateTotals = (rows) => {
      const exSet = new Set();
      let totalVal = 0;
      rows.forEach((r) => {
        if (r.exercicio !== undefined && r.exercicio !== null && r.exercicio !== "") {
          exSet.add(String(r.exercicio));
        }
        const v = Number(r.valor_total || 0);
        if (!Number.isNaN(v)) totalVal += v;
      });
      if (totExercicio) {
        totExercicio.textContent = exSet.size ? Array.from(exSet).sort((a, b) => a.localeCompare(b, "pt-BR")).join(" * ") : "-";
      }
      if (totValorTotal) {
        totValorTotal.textContent = numFmt.format(totalVal);
        totValorTotal.classList.remove("pos", "neg");
        if (totalVal > 0) totValorTotal.classList.add("pos");
        else if (totalVal < 0) totValorTotal.classList.add("neg");
      }
    };

    const renderPagination = (totalPages) => {
      if (!pager) return;
      pager.innerHTML = "";
      if (totalPages <= 1) return;
      const addBtn = (label, page, disabled = false, active = false) => {
        const b = document.createElement("button");
        b.textContent = label;
        if (disabled) b.disabled = true;
        if (active) b.classList.add("active");
        b.addEventListener("click", () => {
          if (disabled || page === currentPage) return;
          currentPage = page;
          renderFiltered(false);
        });
        pager.appendChild(b);
      };
      addBtn("«", 1, currentPage === 1);
      addBtn("‹", Math.max(1, currentPage - 1), currentPage === 1);

      const maxButtons = 5;
      const start = Math.max(1, Math.min(currentPage - 2, totalPages - maxButtons + 1));
      const end = Math.min(totalPages, start + maxButtons - 1);
      for (let p = start; p <= end; p++) {
        addBtn(String(p), p, false, p === currentPage);
      }
      if (end < totalPages) {
        const ellipsis = document.createElement("span");
        ellipsis.textContent = "…";
        pager.appendChild(ellipsis);
        addBtn(String(totalPages), totalPages, false, currentPage === totalPages);
      }

      addBtn("›", Math.min(totalPages, currentPage + 1), currentPage === totalPages);
      addBtn("»", totalPages, currentPage === totalPages);
    };

    const render = () => {
      const rows = filteredRows;
      const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
      if (currentPage > totalPages) currentPage = totalPages;
      const startIdx = (currentPage - 1) * pageSize;
      const pageRows = rows.slice(startIdx, startIdx + pageSize);

      tbody.innerHTML = "";
      pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${r.exercicio ?? ""}</td>
          <td>${r.chave_planejamento ?? ""}</td>
          <td>${r.regiao ?? ""}</td>
          <td>${r.subfuncao_ug ?? ""}</td>
          <td>${r.adj ?? ""}</td>
          <td>${r.macropolitica ?? ""}</td>
          <td>${r.pilar ?? ""}</td>
          <td>${r.eixo ?? ""}</td>
          <td>${r.politica_decreto ?? ""}</td>
          <td>${r.publico_transversal_chave ?? ""}</td>
          <td>${r.programa ?? ""}</td>
          <td>${r.funcao ?? ""}</td>
          <td>${r.unidade_orcamentaria ?? ""}</td>
          <td>${r.acao_paoe ?? ""}</td>
          <td>${r.subfuncao ?? ""}</td>
          <td>${r.objetivo_especifico ?? ""}</td>
          <td>${r.esfera ?? ""}</td>
          <td>${r.responsavel_acao ?? ""}</td>
          <td>${r.produto_acao ?? ""}</td>
          <td>${r.unid_medida_produto ?? ""}</td>
          <td>${r.regiao_produto ?? ""}</td>
          <td>${r.meta_produto ?? ""}</td>
          <td>${r.saldo_meta_produto ?? ""}</td>
          <td>${r.publico_transversal ?? ""}</td>
          <td>${r.subacao_entrega ?? ""}</td>
          <td>${r.responsavel ?? ""}</td>
          <td>${r.prazo ?? ""}</td>
          <td>${r.unid_gestora ?? ""}</td>
          <td>${r.unidade_setorial_planejamento ?? ""}</td>
          <td>${r.produto_subacao ?? ""}</td>
          <td>${r.unidade_medida ?? ""}</td>
          <td>${r.regiao_subacao ?? ""}</td>
          <td>${r.codigo ?? ""}</td>
          <td>${r.municipios_entrega ?? ""}</td>
          <td>${r.meta_subacao ?? ""}</td>
          <td>${r.detalhamento_produto ?? ""}</td>
          <td>${r.etapa ?? ""}</td>
          <td>${r.responsavel_etapa ?? ""}</td>
          <td>${r.prazo_etapa ?? ""}</td>
          <td>${r.regiao_etapa ?? ""}</td>
          <td>${r.natureza ?? ""}</td>
          <td>${r.cat_econ ?? ""}</td>
          <td>${r.grupo ?? ""}</td>
          <td>${r.modalidade ?? ""}</td>
          <td>${r.elemento ?? ""}</td>
          <td>${r.subelemento ?? ""}</td>
          <td>${r.fonte ?? ""}</td>
          <td>${r.idu ?? ""}</td>
          <td>${r.descricao_item_despesa ?? ""}</td>
          <td>${r.unid_medida_item ?? ""}</td>
          <td class="num">${fmtNum(r.quantidade)}</td>
          <td class="num">${fmtNum(r.valor_unitario)}</td>
          <td class="num">${fmtNum(r.valor_total)}</td>
        `;
        tbody.appendChild(tr);
      });

      renderPagination(totalPages);
      updateTotals(rows);
    };

    const allData = { rows: [] };

    const colKeys = [
      "exercicio",
      "chave_planejamento",
      "regiao",
      "subfuncao_ug",
      "adj",
      "macropolitica",
      "pilar",
      "eixo",
      "politica_decreto",
      "publico_transversal_chave",
      "programa",
      "funcao",
      "unidade_orcamentaria",
      "acao_paoe",
      "subfuncao",
      "objetivo_especifico",
      "esfera",
      "responsavel_acao",
      "produto_acao",
      "unid_medida_produto",
      "regiao_produto",
      "meta_produto",
      "saldo_meta_produto",
      "publico_transversal",
      "subacao_entrega",
      "responsavel",
      "prazo",
      "unid_gestora",
      "unidade_setorial_planejamento",
      "produto_subacao",
      "unidade_medida",
      "regiao_subacao",
      "codigo",
      "municipios_entrega",
      "meta_subacao",
      "detalhamento_produto",
      "etapa",
      "responsavel_etapa",
      "prazo_etapa",
      "regiao_etapa",
      "natureza",
      "cat_econ",
      "grupo",
      "modalidade",
      "elemento",
      "subelemento",
      "fonte",
      "idu",
      "descricao_item_despesa",
      "unid_medida_item",
      "quantidade",
      "valor_unitario",
      "valor_total",
    ];

    const filterContainers = table.querySelectorAll(".filter-row [data-col]");
    const filters = Object.fromEntries(colKeys.map((k) => [k, new Set()]));
    const filterControls = {};

    const closeAllPanels = () => {
      Object.values(filterControls).forEach((ctrl) => {
        if (ctrl?.panel) ctrl.panel.classList.remove("open");
      });
    };

    const updateDisplay = (key) => {
      const set = filters[key] || new Set();
      const ctrl = filterControls[key];
      if (!ctrl) return;
      const map = ctrl.labelMap || {};
      if (ctrl.allCb) ctrl.allCb.checked = set.size === 0;
      (ctrl.optionCbs || []).forEach((cb) => {
        cb.checked = set.has(cb.dataset.val || "");
      });
      if (set.size === 0) {
        ctrl.label.textContent = "(Todos)";
      } else if (set.size <= 2) {
        ctrl.label.textContent = Array.from(set)
          .map((v) => map[v] || v)
          .join(", ");
      } else {
        ctrl.label.textContent = `${set.size} selecionados`;
      }
    };

    const buildFilter = (container, options, key) => {
      container.innerHTML = "";
      const wrap = document.createElement("div");
      wrap.className = "mf-wrapper";
      const display = document.createElement("button");
      display.type = "button";
      display.className = "mf-display";
      const label = document.createElement("span");
      label.textContent = "(Todos)";
      display.appendChild(label);
      const icon = document.createElement("i");
      icon.className = "bi bi-chevron-down";
      display.appendChild(icon);

      const panel = document.createElement("div");
      panel.className = "mf-panel";
      const search = document.createElement("input");
      search.type = "text";
      search.className = "mf-search";
      search.placeholder = "Buscar...";
      const list = document.createElement("div");
      list.className = "mf-options";

      const tempSelected = new Set(filters[key] || []);
      const allId = `${key}-all`;
      const allRow = document.createElement("label");
      allRow.className = "mf-option";
      const allCb = document.createElement("input");
      allCb.type = "checkbox";
      allCb.id = allId;
      allCb.dataset.val = "";
      allRow.appendChild(allCb);
      const allSpan = document.createElement("span");
      allSpan.textContent = "(Todos)";
      allRow.appendChild(allSpan);
      list.appendChild(allRow);

      const selectVisibleRow = document.createElement("label");
      selectVisibleRow.className = "mf-option mf-select-visible";
      const selectVisibleCb = document.createElement("input");
      selectVisibleCb.type = "checkbox";
      selectVisibleRow.appendChild(selectVisibleCb);
      const selectVisibleSpan = document.createElement("span");
      selectVisibleSpan.textContent = "Selecionar exibidos";
      selectVisibleRow.appendChild(selectVisibleSpan);
      list.appendChild(selectVisibleRow);

      const cbs = [];
      const labelMap = {};
      options.forEach((opt) => {
        const row = document.createElement("label");
        row.className = "mf-option";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        const norm = String(opt || "").toLowerCase();
        cb.dataset.val = norm;
        labelMap[norm] = opt;
        row.appendChild(cb);
        const txt = document.createElement("span");
        txt.textContent = opt;
        row.appendChild(txt);
        list.appendChild(row);
        cbs.push({ cb, txt, row, val: norm });
      });

      const syncUIFromTemp = () => {
        allCb.checked = tempSelected.size === 0;
        cbs.forEach(({ cb, val }) => {
          cb.checked = tempSelected.has(val);
        });
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        const allVisibleSelected = visible.length > 0 && visible.every(({ cb }) => cb.checked);
        selectVisibleCb.checked = allVisibleSelected;
      };

      const applyTempToFilters = () => {
        const set = filters[key];
        set.clear();
        tempSelected.forEach((v) => set.add(v));
        updateDisplay(key);
        renderFiltered();
      };

      const closePanel = () => panel.classList.remove("open");

      allCb.addEventListener("change", () => {
        if (allCb.checked) {
          tempSelected.clear();
          syncUIFromTemp();
        }
      });

      selectVisibleCb.addEventListener("change", () => {
        const visible = cbs.filter(({ row }) => row.style.display !== "none");
        if (selectVisibleCb.checked) {
          visible.forEach(({ val }) => tempSelected.add(val));
        } else {
          visible.forEach(({ val }) => tempSelected.delete(val));
        }
        allCb.checked = tempSelected.size === 0;
        syncUIFromTemp();
      });

      cbs.forEach(({ cb, val }) => {
        cb.addEventListener("change", () => {
          if (cb.checked) {
            tempSelected.add(val);
            allCb.checked = false;
          } else {
            tempSelected.delete(val);
          }
          syncUIFromTemp();
        });
      });

      search.addEventListener("input", () => {
        const term = search.value.toLowerCase();
        cbs.forEach(({ row, txt }) => {
          const match = txt.textContent.toLowerCase().includes(term);
          row.style.display = match ? "" : "none";
        });
        const allMatch = "(todos)".includes(term) || term === "";
        allRow.style.display = allMatch ? "" : "none";
        selectVisibleRow.style.display = "";
        syncUIFromTemp();
      });

      const actions = document.createElement("div");
      actions.className = "mf-actions";
      const cancelBtn = document.createElement("button");
      cancelBtn.type = "button";
      cancelBtn.className = "mf-btn ghost";
      cancelBtn.textContent = "Cancelar";
      const applyBtn = document.createElement("button");
      applyBtn.type = "button";
      applyBtn.className = "mf-btn primary";
      applyBtn.textContent = "Aplicar";

      cancelBtn.addEventListener("click", () => {
        tempSelected.clear();
        filters[key].forEach((v) => tempSelected.add(v));
        syncUIFromTemp();
        closePanel();
      });
      applyBtn.addEventListener("click", () => {
        applyTempToFilters();
        closePanel();
      });

      display.addEventListener("click", () => {
        const isOpen = panel.classList.contains("open");
        closeAllPanels();
        if (!isOpen) {
          panel.style.width = "";
          panel.style.height = "";
          tempSelected.clear();
          filters[key].forEach((v) => tempSelected.add(v));
          cbs.forEach(({ row }) => (row.style.display = ""));
          allRow.style.display = "";
          search.value = "";
          syncUIFromTemp();
          panel.classList.add("open");
        }
      });

      wrap.appendChild(display);
      panel.appendChild(search);
      panel.appendChild(list);
      actions.appendChild(cancelBtn);
      actions.appendChild(applyBtn);
      panel.appendChild(actions);
      wrap.appendChild(panel);
      container.appendChild(wrap);

      filterControls[key] = {
        panel,
        label,
        allCb,
        optionCbs: cbs.map((c) => c.cb),
        labelMap,
      };
      updateDisplay(key);
    };

    const setOptions = (rows = allData.rows) => {
      closeAllPanels();
      const uniques = colKeys.map(() => new Set());
      (rows || []).forEach((r) => {
        colKeys.forEach((k, idx) => {
          const v = r[k];
          if (v !== undefined && v !== null && v !== "") uniques[idx].add(String(v));
        });
      });
      filterContainers.forEach((container) => {
        const key = container.getAttribute("data-col");
        const idx = colKeys.indexOf(key);
        if (idx === -1) return;
        const opts = Array.from(uniques[idx]).sort((a, b) => a.localeCompare(b, "pt-BR"));
        buildFilter(container, opts, key);
      });
    };

    const renderFiltered = (resetPage = true) => {
      const filtered = allData.rows.filter((r) =>
        colKeys.every((k) => {
          const set = filters[k];
          if (!set || set.size === 0) return true;
          const val = r[k];
          const cmp = val === null || val === undefined ? "" : String(val).toLowerCase();
          return set.has(cmp);
        })
      );
      setOptions(filtered);
      filteredRows = filtered;
      if (resetPage) currentPage = 1;
      render();
    };

    if (!multiFilterClickBound) {
      document.addEventListener("click", (ev) => {
        if (!ev.target.closest(".mf-wrapper")) {
          closeAllPanels();
        }
      });
      multiFilterClickBound = true;
    }

    const load = async () => {
      if (meta) meta.textContent = "Carregando...";
      try {
        const res = await fetch("/api/relatorios/plan20-seduc");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "Falha ao carregar.");
        allData.rows = data.data || [];
        setOptions(allData.rows);
        filteredRows = allData.rows;
        render();
        if (meta) {
          const dt = data.data_arquivo ? new Date(data.data_arquivo).toLocaleString("pt-BR") : "-";
          const user = data.user_email || "-";
          const uploaded = data.uploaded_at ? new Date(data.uploaded_at).toLocaleString("pt-BR") : "-";
          meta.innerHTML = `
            <div><strong>Última atualização</strong></div>
            <div>Enviado por: ${user}</div>
            <div>Upload em: ${uploaded}</div>
            <div>Data do download: ${dt}</div>
          `;
        }
      } catch (err) {
        if (meta) meta.textContent = err.message;
        console.error(err);
      }
    };

    load();

    if (btnReset) {
      btnReset.addEventListener("click", () => {
        Object.keys(filters).forEach((k) => filters[k].clear());
        setOptions(allData.rows);
        filteredRows = allData.rows;
        currentPage = 1;
        render();
      });
    }

    if (pageSizeSelect) {
      pageSizeSelect.addEventListener("change", () => {
        const val = parseInt(pageSizeSelect.value || "20", 10);
        pageSize = Number.isNaN(val) ? 20 : val;
        currentPage = 1;
        render();
      });
    }

    if (btnDownload) {
      btnDownload.addEventListener("click", () => {
        window.open("/api/relatorios/plan20-seduc/download", "_blank");
      });
    }
  }

  function initRoute(route) {
    if (route === "usuarios" || route === "usuarios/cadastrar") {
      initUsuariosForm();
    }
    if (route === "usuarios/editar") {
      initUsuariosEditar();
    }
    if (route === "usuarios/perfil") {
      initPerfis();
    }
    if (route === "usuarios/senha") {
      initUsuariosSenha();
    }
    if (route === "painel") {
      initPainel();
    }
    if (route === "atualizar/fip613") {
      initFip613();
    }
    if (route === "atualizar/plan20-seduc") {
      initPlan20();
    }
    if (route === "relatorios/fip613") {
      initRelatorioFip();
    }
    if (route === "relatorios/plan20-seduc") {
      initRelatorioPlan20();
    }
  }

  if (menu) {
    menu.addEventListener("click", (ev) => {
      const parentToggle = ev.target.closest(".menu-parent[data-submenu]");
      if (parentToggle) {
        const targetId = parentToggle.getAttribute("data-submenu");
        const group = parentToggle.closest(".menu-group");
        const isOpen = group?.classList.contains("open");
        document.querySelectorAll(".menu-group").forEach((g) => {
          if (g !== group) g.classList.remove("open");
        });
        if (group) {
          if (isOpen) {
            group.classList.remove("open");
          } else if (targetId) {
            const submenu = document.getElementById(targetId);
            if (submenu) group.classList.add("open");
          }
        }
        return;
      }

      const link = ev.target.closest("[data-route]");
      if (!link) return;
      ev.preventDefault();
      const route = link.getAttribute("data-route");
      setActive(route);
      loadPage(route);
    });
  }

  setUserMeta();
  fetchCurrentPermissions();

  if (content) {
    const initial = content.dataset.initial || "dashboard";
    setActive(initial);
    loadPage(initial);
  }
})();
    const negateCols = new Set([
      "reducao",
      "bloqueado_conting",
      "reserva_empenho",
      "empenhado",
    ]);
    const adjustVal = (k, v) => (negateCols.has(k) ? Number(v || 0) * -1 : Number(v || 0));

